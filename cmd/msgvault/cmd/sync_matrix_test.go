package cmd

import (
	"bytes"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/config"
	matrixsource "go.kenn.io/msgvault/internal/matrix"
	"go.kenn.io/msgvault/internal/store"
	"go.kenn.io/msgvault/internal/testutil"
)

func TestMatrixSyncSummaryBackupFailureAndKeyHint(t *testing.T) {
	setup := func(t *testing.T) (*store.Store, *config.Config, matrixsource.Credentials, *bool) {
		t.Helper()
		req := require.New(t)
		newPlaceholder := new(bool)
		mux := http.NewServeMux()
		mux.HandleFunc("GET /_matrix/client/v3/sync", func(w http.ResponseWriter, _ *http.Request) {
			if *newPlaceholder {
				_, _ = w.Write([]byte(`{"next_batch":"next","rooms":{"join":{"!room:example.org":{"timeline":{"events":[{"type":"m.room.encrypted","event_id":"$new","sender":"@member:example.org","content":{"algorithm":"m.megolm.v1.aes-sha2","session_id":"unknown","ciphertext":"unknown"}}]}}}}}`))
			} else {
				_, _ = w.Write([]byte(`{"next_batch":"next","rooms":{"join":{}}}`))
			}
		})
		mux.HandleFunc("GET /_matrix/client/v3/user/{user}/account_data/m.direct", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{}`))
		})
		mux.HandleFunc("GET /_matrix/client/v3/room_keys/version", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
		})
		mux.HandleFunc("GET /_matrix/client/v3/rooms/{room}/joined_members", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"joined":{}}`))
		})
		mux.HandleFunc("GET /_matrix/client/v3/rooms/{room}/messages", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"chunk":[]}`))
		})
		server := httptest.NewServer(mux)
		t.Cleanup(server.Close)
		s := testutil.NewTestStore(t)
		source, err := s.GetOrCreateSource(sourceTypeMatrix, "@archive:example.org")
		req.NoError(err)
		convID, err := s.EnsureConversation(source.ID, "!room:example.org", "Room")
		req.NoError(err)
		_, err = s.PersistMessageContext(t.Context(), &store.MessagePersistData{Message: &store.Message{SourceID: source.ID, ConversationID: convID, SourceMessageID: "$pending", MessageType: "matrix", Snippet: sql.NullString{String: store.MatrixEncryptedPlaceholder, Valid: true}}})
		req.NoError(err)
		ids, err := s.MessageExistsBatch(source.ID, []string{"$pending"})
		req.NoError(err)
		req.NoError(s.UpsertMessageRawWithFormat(ids["$pending"], []byte(`{"type":"m.room.encrypted"}`), "matrix_json"))
		home := t.TempDir()
		cfg := &config.Config{HomeDir: home, Data: config.DataConfig{DataDir: home}}
		creds := matrixsource.Credentials{Homeserver: server.URL, UserID: source.Identifier, DeviceID: "ARCHIVE", AccessToken: "token"}
		return s, cfg, creds, newPlaceholder
	}
	t.Run("saved backup key overrides skip preference", func(t *testing.T) {
		req := require.New(t)
		s, cfg, creds, _ := setup(t)
		source, err := s.GetSourceByTypeAndIdentifier(sourceTypeMatrix, creds.UserID)
		req.NoError(err)
		req.NoError(s.UpdateSourceSyncConfig(source.ID, `{"skip_key_backup":true}`))
		creds.BackupKey = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
		req.NoError(matrixsource.SaveCredentials(cfg.TokensDir(), creds))
		var out bytes.Buffer
		req.ErrorContains(runMatrixSync(t.Context(), s, cfg, "", false, nil, &out), "key backup unavailable, 1 encrypted events pending")
	})
	for _, mode := range []string{"single hint across sources", "exclude rooms"} {
		t.Run(mode, func(t *testing.T) {
			req, check := require.New(t), assert.New(t)
			s, cfg, creds, newPlaceholder := setup(t)
			req.NoError(matrixsource.SaveCredentials(cfg.TokensDir(), creds))
			var out bytes.Buffer
			switch mode {
			case "exclude rooms":
				req.NoError(runMatrixSync(t.Context(), s, cfg, "", false, nil, &out))
				check.Contains(out.String(), "--recovery-file")
				cfg.Matrix.ExcludeRooms = []string{"!room:example.org"}
				out.Reset()
				req.NoError(runMatrixSync(t.Context(), s, cfg, "", false, nil, &out))
				check.NotContains(out.String(), "--recovery-file")
			default:
				second, err := s.GetOrCreateSource(sourceTypeMatrix, "@second:example.org")
				req.NoError(err)
				secondRoom, err := s.EnsureConversation(second.ID, "!room:example.org", "Room")
				req.NoError(err)
				_, err = s.PersistMessageContext(t.Context(), &store.MessagePersistData{Message: &store.Message{SourceID: second.ID, ConversationID: secondRoom, SourceMessageID: "$second", MessageType: "matrix", Snippet: sql.NullString{String: store.MatrixEncryptedPlaceholder, Valid: true}}, RawMIME: []byte(`{"type":"m.room.encrypted"}`), RawFormat: "matrix_json"})
				req.NoError(err)
				req.NoError(matrixsource.SaveCredentials(cfg.TokensDir(), matrixsource.Credentials{Homeserver: creds.Homeserver, UserID: second.Identifier, DeviceID: "ARCHIVE", AccessToken: "token"}))
				*newPlaceholder = true
				req.NoError(runMatrixSync(t.Context(), s, cfg, "", false, nil, &out))
				check.Equal(1, strings.Count(out.String(), "run msgvault add-matrix --homeserver"))
			}
		})
	}
	t.Run("unreadable preference", func(t *testing.T) {
		req, check := require.New(t), assert.New(t)
		s, cfg, creds, newPlaceholder := setup(t)
		*newPlaceholder = true
		sources, err := s.ListSources(sourceTypeMatrix)
		req.NoError(err)
		req.NoError(s.UpdateSourceSyncConfig(sources[0].ID, `"invalid JSON"`))
		req.NoError(matrixsource.SaveCredentials(cfg.TokensDir(), creds))
		var out bytes.Buffer
		req.NoError(runMatrixSync(t.Context(), s, cfg, "", false, nil, &out))
		check.Contains(out.String(), "run msgvault add-matrix --homeserver")
	})
	t.Run("skip key backup", func(t *testing.T) {
		req, check := require.New(t), assert.New(t)
		s, cfg, creds, _ := setup(t)
		sources, err := s.ListSources(sourceTypeMatrix)
		req.NoError(err)
		req.NoError(s.UpdateSourceSyncConfig(sources[0].ID, `{"skip_key_backup":true}`))
		req.NoError(matrixsource.SaveCredentials(cfg.TokensDir(), creds))
		var out bytes.Buffer
		req.NoError(runMatrixSync(t.Context(), s, cfg, "", false, nil, &out))
		check.Contains(out.String(), "1 encrypted placeholders")
		check.NotContains(out.String(), "--recovery-file")
	})
}
