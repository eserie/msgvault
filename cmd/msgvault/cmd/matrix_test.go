package cmd

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/clirun"
	"go.kenn.io/msgvault/internal/config"
	matrixsource "go.kenn.io/msgvault/internal/matrix"
	"go.kenn.io/msgvault/internal/store"
	"go.kenn.io/msgvault/internal/testutil"
)

func TestReadMatrixSecretFilePreservesPasswordWhitespace(t *testing.T) {
	require := require.New(t)
	path := filepath.Join(t.TempDir(), "password")
	require.NoError(os.WriteFile(path, []byte("  password with spaces  \r\n"), 0o600))

	password, err := readMatrixSecretFile(path, false)
	require.NoError(err)
	assert.Equal(t, "  password with spaces  ", password)
	token, err := readMatrixSecretFile(path, true)
	require.NoError(err)
	assert.Equal(t, "password with spaces", token)
}

func TestRunConfiguredMatrixSyncRefreshesCacheAfterFailedAttempt(t *testing.T) {
	require := require.New(t)
	st := testutil.NewTestStore(t)
	cfg := &config.Config{HomeDir: t.TempDir(), Data: config.DataConfig{DataDir: t.TempDir()}}
	ctx := testInvocationContext(t.Context(), cfg, invocationOptions{})

	original := rebuildMatrixCacheAfterScheduledSync
	t.Cleanup(func() { rebuildMatrixCacheAfterScheduledSync = original })
	refreshErr := errors.New("synthetic cache refresh failure")
	var calls int
	rebuildMatrixCacheAfterScheduledSync = func(gotCtx context.Context, label string) error {
		calls++
		assert.Equal(t, "matrix", label)
		assert.NoError(t, gotCtx.Err())
		return refreshErr
	}

	err := runConfiguredMatrixSync(ctx, st)
	require.ErrorContains(err, "no Matrix accounts registered")
	require.ErrorIs(err, refreshErr)
	assert.Equal(t, 1, calls)
}

func TestAddMatrixRenewsExistingAccountInPlace(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	var loggedOut []string
	mux := http.NewServeMux()
	mux.HandleFunc("POST /_matrix/client/v3/login", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"user_id":"@archive:example.org","device_id":"NEW","access_token":"fresh"}`))
	})
	mux.HandleFunc("POST /_matrix/client/v3/logout", func(w http.ResponseWriter, r *http.Request) {
		loggedOut = append(loggedOut, r.Header.Get("Authorization"))
		_, _ = w.Write([]byte(`{}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	home := t.TempDir()
	cfg := &config.Config{HomeDir: home, Data: config.DataConfig{DataDir: home}}
	require.NoError(matrixsource.SaveCredentials(cfg.TokensDir(), matrixsource.Credentials{
		Homeserver: server.URL, UserID: "@archive:example.org", DeviceID: "OLD", AccessToken: "revoked",
	}))
	st, err := store.Open(cfg.DatabaseDSN())
	require.NoError(err)
	require.NoError(st.InitSchema())
	source, err := st.GetOrCreateSource(sourceTypeMatrix, "@archive:example.org")
	require.NoError(err)
	convID, err := st.EnsureConversation(source.ID, "!room:example.org", "Room")
	require.NoError(err)
	_, err = st.UpsertMessage(&store.Message{ConversationID: convID, SourceID: source.ID, SourceMessageID: "$kept", MessageType: sourceTypeMatrix})
	require.NoError(err)
	require.NoError(st.Close())

	t.Setenv(daemonCLISubprocessEnv, strconv.Itoa(os.Getppid()))
	t.Setenv(clirun.EnvMatrixLoginSecret, "password")
	root := newTestRootCmd()
	root.AddCommand(newAddMatrixCmd())
	root.SetArgs([]string{"add-matrix", "--homeserver", server.URL, "--user-id", "@archive:example.org", "--no-default-identity"})
	root.SetContext(testInvocationContext(t.Context(), cfg, invocationOptions{}))
	root.SetOut(io.Discard)
	root.SetErr(io.Discard)
	require.NoError(root.Execute())

	creds, err := matrixsource.LoadCredentials(cfg.TokensDir(), "@archive:example.org")
	require.NoError(err)
	assert.Equal("NEW", creds.DeviceID)
	assert.Equal([]string{"Bearer revoked"}, loggedOut)
	st, err = store.Open(cfg.DatabaseDSN())
	require.NoError(err)
	defer func() { _ = st.Close() }()
	renewed, err := st.GetOrCreateSource(sourceTypeMatrix, "@archive:example.org")
	require.NoError(err)
	assert.Equal(source.ID, renewed.ID)
	count, err := st.CountMessagesForSource(source.ID)
	require.NoError(err)
	assert.Equal(int64(1), count)
}

func TestAddMatrixKeepsOldLoginWhenPreviousLogoutFails(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	var loggedOut []string
	mux := http.NewServeMux()
	mux.HandleFunc("POST /_matrix/client/v3/login", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"user_id":"@archive:example.org","device_id":"NEW","access_token":"fresh"}`))
	})
	mux.HandleFunc("POST /_matrix/client/v3/logout", func(w http.ResponseWriter, r *http.Request) {
		loggedOut = append(loggedOut, r.Header.Get("Authorization"))
		if r.Header.Get("Authorization") == "Bearer still-valid" {
			http.Error(w, `{"errcode":"M_UNKNOWN","error":"temporary failure"}`, http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	home := t.TempDir()
	cfg := &config.Config{HomeDir: home, Data: config.DataConfig{DataDir: home}}
	old := matrixsource.Credentials{Homeserver: server.URL, UserID: "@archive:example.org", DeviceID: "OLD", AccessToken: "still-valid"}
	require.NoError(matrixsource.SaveCredentials(cfg.TokensDir(), old))

	t.Setenv(daemonCLISubprocessEnv, strconv.Itoa(os.Getppid()))
	t.Setenv(clirun.EnvMatrixLoginSecret, "password")
	root := newTestRootCmd()
	root.AddCommand(newAddMatrixCmd())
	root.SetArgs([]string{"add-matrix", "--homeserver", server.URL, "--user-id", "@archive:example.org", "--no-default-identity"})
	root.SetContext(testInvocationContext(t.Context(), cfg, invocationOptions{}))
	root.SetOut(io.Discard)
	root.SetErr(io.Discard)
	require.ErrorContains(root.Execute(), "login unchanged")

	creds, err := matrixsource.LoadCredentials(cfg.TokensDir(), "@archive:example.org")
	require.NoError(err)
	assert.Equal(old, creds, "the old login stays so revocation can be retried")
	assert.Equal([]string{"Bearer still-valid", "Bearer fresh"}, loggedOut, "the new device is logged out again")
}

func TestAddMatrixStopsWhenExistingLoginIsUnreadable(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	var loggedOut []string
	mux := http.NewServeMux()
	mux.HandleFunc("POST /_matrix/client/v3/login", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"user_id":"@archive:example.org","device_id":"NEW","access_token":"fresh"}`))
	})
	mux.HandleFunc("POST /_matrix/client/v3/logout", func(w http.ResponseWriter, r *http.Request) {
		loggedOut = append(loggedOut, r.Header.Get("Authorization"))
		_, _ = w.Write([]byte(`{}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	home := t.TempDir()
	cfg := &config.Config{HomeDir: home, Data: config.DataConfig{DataDir: home}}
	require.NoError(matrixsource.SaveCredentials(cfg.TokensDir(), matrixsource.Credentials{
		Homeserver: server.URL, UserID: "@archive:example.org", DeviceID: "OLD", AccessToken: "old",
	}))
	matches, err := filepath.Glob(filepath.Join(cfg.TokensDir(), "matrix_*.json"))
	require.NoError(err)
	require.Len(matches, 1)
	require.NoError(os.WriteFile(matches[0], []byte("not json"), 0o600))

	t.Setenv(daemonCLISubprocessEnv, strconv.Itoa(os.Getppid()))
	t.Setenv(clirun.EnvMatrixLoginSecret, "password")
	root := newTestRootCmd()
	root.AddCommand(newAddMatrixCmd())
	root.SetArgs([]string{"add-matrix", "--homeserver", server.URL, "--user-id", "@archive:example.org", "--no-default-identity"})
	root.SetContext(testInvocationContext(t.Context(), cfg, invocationOptions{}))
	root.SetOut(io.Discard)
	root.SetErr(io.Discard)
	require.ErrorContains(root.Execute(), "existing Matrix login")

	data, err := os.ReadFile(matches[0])
	require.NoError(err)
	assert.Equal("not json", string(data), "the unreadable file is left for the user to fix")
	assert.Equal([]string{"Bearer fresh"}, loggedOut, "only the new device is logged out again")
}
