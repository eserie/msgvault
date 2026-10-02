package cmd

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/config"
	matrixsource "go.kenn.io/msgvault/internal/matrix"
	"go.kenn.io/msgvault/internal/testutil"
)

func TestCleanupFreshMatrixDevicePreservesCredentialWhenLogoutFails(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "temporary failure", http.StatusServiceUnavailable)
	}))
	t.Cleanup(server.Close)
	tokensDir := t.TempDir()
	creds := matrixsource.Credentials{
		Homeserver: server.URL, UserID: "@archive:example.org",
		DeviceID: "ARCHIVEDEVICE", AccessToken: "synthetic-token",
	}

	err := cleanupFreshMatrixDevice(t.Context(), tokensDir, creds)

	require.ErrorContains(err, "clean up new Matrix device")
	got, loadErr := matrixsource.LoadCredentials(tokensDir, creds.UserID)
	require.NoError(loadErr)
	assert.Equal(creds, got)
}

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
