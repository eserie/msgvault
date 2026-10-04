package whatsapp

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/testutil"
)

// createAppleMarkerFixture adds the Core Data Z_OPT columns and Z_METADATA
// store identity that a real ChatStorage.sqlite carries to the shared chat
// fixture.
func createAppleMarkerFixture(t *testing.T) string {
	t.Helper()
	path := createAppleChatFixture(t)
	execAppleFixture(t, path, `
		ALTER TABLE ZWAMESSAGE ADD COLUMN Z_OPT INTEGER DEFAULT 1;
		ALTER TABLE ZWAGROUPMEMBER ADD COLUMN Z_OPT INTEGER DEFAULT 1;
		UPDATE ZWAMESSAGE SET Z_OPT = 1;
		UPDATE ZWAGROUPMEMBER SET Z_OPT = 1;
		CREATE TABLE Z_METADATA (Z_VERSION INTEGER PRIMARY KEY, Z_UUID VARCHAR(255), Z_PLIST BLOB);
		INSERT INTO Z_METADATA VALUES (1, 'store-uuid-a', NULL);
	`)
	return path
}

// stealthEditApple changes a message's text without bumping Z_OPT, so the
// edit is visible only to an import that reads the row.
func stealthEditApple(t *testing.T, path string, rowID int, text string) {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	_, err = db.Exec(`UPDATE ZWAMESSAGE SET ZTEXT = ? WHERE Z_PK = ?`, text, rowID)
	require.NoError(t, err)
	require.NoError(t, db.Close())
}

func TestImportAppleRerunSkipsUnchangedChats(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	chatDBPath := createAppleMarkerFixture(t)
	createAppleLIDFixture(t, filepath.Dir(chatDBPath))
	st := testutil.NewTestStore(t)
	importer := NewImporter(st, nil)
	first, err := importer.Import(context.Background(), chatDBPath, appleTestOptions())
	require.NoError(err)
	require.Equal(int64(4), first.MessagesAdded)
	markAppleMessagesUnwritten(t, st)

	// The rerun must not read chat 1: had it read the row, it would rewrite it.
	stealthEditApple(t, chatDBPath, 1, "unseen edit")
	second, err := importer.Import(context.Background(), chatDBPath, appleTestOptions())
	require.NoError(err)
	assert.Zero(second.MessagesProcessed)
	assert.Zero(second.MessagesAdded)
	assert.Empty(rewrittenAppleMessages(t, st))
	assert.Equal("direct prototype text", appleBodyText(t, st, "direct-in"))

	full := appleTestOptions()
	full.Full = true
	forced, err := importer.Import(context.Background(), chatDBPath, full)
	require.NoError(err)
	assert.Equal(int64(1), forced.MessagesAdded)
	assert.Equal([]string{"direct-in"}, rewrittenAppleMessages(t, st))
	assert.Equal("unseen edit", appleBodyText(t, st, "direct-in"))

	// --full records fresh markers, so the next run skips every chat again.
	after, err := importer.Import(context.Background(), chatDBPath, appleTestOptions())
	require.NoError(err)
	assert.Zero(after.MessagesProcessed)
}

func TestImportAppleRerunReadsChangedChats(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	chatDBPath := createAppleMarkerFixture(t)
	createAppleLIDFixture(t, filepath.Dir(chatDBPath))
	st := testutil.NewTestStore(t)
	importer := NewImporter(st, nil)
	_, err := importer.Import(context.Background(), chatDBPath, appleTestOptions())
	require.NoError(err)
	markAppleMessagesUnwritten(t, st)

	// Chat 1: an edit as Core Data saves it. Chat 3: a new message. Chat 2: a
	// deletion, which makes the rerun read a stealth edit in the same chat.
	execAppleFixture(t, chatDBPath, `
		UPDATE ZWAMESSAGE SET ZTEXT = 'edited direct text', Z_OPT = Z_OPT + 1 WHERE Z_PK = 1;
		INSERT INTO ZWAMESSAGE VALUES
			(20, 3, NULL, 'lid-new', 0, 700000020, 'new lid text', 0, '999999999999999@lid', 1);
		DELETE FROM ZWAMESSAGE WHERE Z_PK = 4;
		UPDATE ZWAMESSAGE SET ZTEXT = 'group text read again' WHERE Z_PK = 3;
	`)
	second, err := importer.Import(context.Background(), chatDBPath, appleTestOptions())
	require.NoError(err)
	assert.Equal(int64(3), second.MessagesAdded)
	assert.Equal([]string{"direct-in", "group-in", "lid-new"}, rewrittenAppleMessages(t, st))
	assert.Equal("edited direct text", appleBodyText(t, st, "direct-in"))
	assert.Equal("new lid text", appleBodyText(t, st, "lid-new"))
	assert.Equal("group text read again", appleBodyText(t, st, "group-in"))
	// Messages deleted in WhatsApp stay archived.
	assertStoreCount(t, st.DB(), "messages", 5)

	third, err := importer.Import(context.Background(), chatDBPath, appleTestOptions())
	require.NoError(err)
	assert.Zero(third.MessagesProcessed)
}

func TestImportAppleRerunReadsChatsWithGroupMemberChange(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	chatDBPath := createAppleMarkerFixture(t)
	createAppleLIDFixture(t, filepath.Dir(chatDBPath))
	st := testutil.NewTestStore(t)
	importer := NewImporter(st, nil)
	_, err := importer.Import(context.Background(), chatDBPath, appleTestOptions())
	require.NoError(err)
	markAppleMessagesUnwritten(t, st)

	// The group member's name is part of each of its messages' raw payload.
	execAppleFixture(t, chatDBPath, `
		UPDATE ZWAGROUPMEMBER SET ZCONTACTNAME = 'Bob Renamed', Z_OPT = Z_OPT + 1 WHERE Z_PK = 10
	`)
	summary, err := importer.Import(context.Background(), chatDBPath, appleTestOptions())
	require.NoError(err)
	assert.Equal(int64(1), summary.MessagesAdded)
	assert.Equal([]string{"group-in"}, rewrittenAppleMessages(t, st))
}

func TestImportAppleRerunInvalidatesMarkers(t *testing.T) {
	tests := []struct {
		name   string
		change func(t *testing.T, chatDBPath string, sourceID int64, importer *Importer)
	}{
		{
			name: "LID mapping appears",
			change: func(t *testing.T, chatDBPath string, _ int64, _ *Importer) {
				t.Helper()
				createAppleLIDFixture(t, filepath.Dir(chatDBPath))
			},
		},
		{
			name: "another sync completes on the source",
			change: func(t *testing.T, _ string, sourceID int64, importer *Importer) {
				t.Helper()
				syncID, err := importer.store.StartSync(sourceID, "whatsapp_import")
				require.NoError(t, err)
				require.NoError(t, importer.store.CompleteSync(syncID, ""))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			assert := assert.New(t)

			chatDBPath := createAppleMarkerFixture(t)
			st := testutil.NewTestStore(t)
			importer := NewImporter(st, nil)
			first, err := importer.Import(context.Background(), chatDBPath, appleTestOptions())
			require.NoError(err)
			markAppleMessagesUnwritten(t, st)

			tt.change(t, chatDBPath, first.SourceID, importer)
			stealthEditApple(t, chatDBPath, 2, "outbound read again")
			summary, err := importer.Import(context.Background(), chatDBPath, appleTestOptions())
			require.NoError(err)
			assert.Contains(rewrittenAppleMessages(t, st), "direct-out")
			assert.Equal("outbound read again", appleBodyText(t, st, "direct-out"))
			assert.Equal(first.MessagesProcessed, summary.MessagesProcessed)
		})
	}
}

func TestImportAppleRerunReadsDifferentDatabase(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	// Same phone, same per-chat aggregates, different store and messages.
	first := createAppleMarkerFixture(t)
	other := createAppleMarkerFixture(t)
	execAppleFixture(t, other, `
		UPDATE Z_METADATA SET Z_UUID = 'store-uuid-b';
		UPDATE ZWAMESSAGE SET ZTEXT = 'other database text' WHERE Z_PK = 1;
	`)
	st := testutil.NewTestStore(t)
	importer := NewImporter(st, nil)
	_, err := importer.Import(context.Background(), first, appleTestOptions())
	require.NoError(err)
	markAppleMessagesUnwritten(t, st)

	_, err = importer.Import(context.Background(), other, appleTestOptions())
	require.NoError(err)
	assert.Contains(rewrittenAppleMessages(t, st), "direct-in")
	assert.Equal("other database text", appleBodyText(t, st, "direct-in"))
}

func TestImportAppleRerunReadsChatsAfterRowSwap(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	chatDBPath := createAppleMarkerFixture(t)
	// Rows 30 and 31 pin each chat's highest Z_PK, so a swap leaves count,
	// highest Z_PK and Z_OPT total unchanged.
	execAppleFixture(t, chatDBPath, `
		INSERT INTO ZWAMESSAGE (Z_PK, ZCHATSESSION, ZSTANZAID, ZISFROMME, ZMESSAGEDATE, ZTEXT, ZMESSAGETYPE, ZFROMJID, Z_OPT)
		VALUES (30, 1, 'pin-direct', 0, 700000030, 'pin direct', 0, '15555550101@s.whatsapp.net', 1),
		       (31, 2, 'pin-group', 0, 700000031, 'pin group', 0, '120363000000000000@g.us', 1);
	`)
	st := testutil.NewTestStore(t)
	importer := NewImporter(st, nil)
	_, err := importer.Import(context.Background(), chatDBPath, appleTestOptions())
	require.NoError(err)
	markAppleMessagesUnwritten(t, st)

	execAppleFixture(t, chatDBPath, `
		UPDATE ZWAMESSAGE SET ZCHATSESSION = 2 WHERE Z_PK = 2;
		UPDATE ZWAMESSAGE SET ZCHATSESSION = 1 WHERE Z_PK = 3;
	`)
	_, err = importer.Import(context.Background(), chatDBPath, appleTestOptions())
	require.NoError(err)
	assert.Contains(rewrittenAppleMessages(t, st), "direct-out")
	assert.Contains(rewrittenAppleMessages(t, st), "group-in")
}

func TestImportAppleRerunReadsChatsWithoutMarkers(t *testing.T) {
	tests := []struct {
		name    string
		fixture func(t *testing.T) string
	}{
		{name: "no Z_OPT columns", fixture: createAppleChatFixture},
		{
			name: "no store identity",
			fixture: func(t *testing.T) string {
				t.Helper()
				path := createAppleMarkerFixture(t)
				execAppleFixture(t, path, `DROP TABLE Z_METADATA`)
				return path
			},
		},
		{
			name: "row without Z_OPT",
			fixture: func(t *testing.T) string {
				t.Helper()
				path := createAppleMarkerFixture(t)
				execAppleFixture(t, path, `UPDATE ZWAMESSAGE SET Z_OPT = NULL WHERE Z_PK = 7`)
				return path
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			assert := assert.New(t)

			chatDBPath := tt.fixture(t)
			createAppleLIDFixture(t, filepath.Dir(chatDBPath))
			st := testutil.NewTestStore(t)
			importer := NewImporter(st, nil)
			_, err := importer.Import(context.Background(), chatDBPath, appleTestOptions())
			require.NoError(err)
			markAppleMessagesUnwritten(t, st)

			stealthEditApple(t, chatDBPath, 1, "edit without marker")
			summary, err := importer.Import(context.Background(), chatDBPath, appleTestOptions())
			require.NoError(err)
			assert.Equal(int64(1), summary.MessagesAdded)
			assert.Equal([]string{"direct-in"}, rewrittenAppleMessages(t, st))
		})
	}
}

func TestImportAppleLimitLeavesTruncatedChatUnmarked(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	chatDBPath := createAppleMarkerFixture(t)
	createAppleLIDFixture(t, filepath.Dir(chatDBPath))
	// Chat 1 comes first, so the limit stops between its two messages.
	execAppleFixture(t, chatDBPath, `UPDATE ZWACHATSESSION SET ZLASTMESSAGEDATE = 800000000 WHERE Z_PK = 1`)
	st := testutil.NewTestStore(t)
	importer := NewImporter(st, nil)
	limited := appleTestOptions()
	limited.Limit = 1
	_, err := importer.Import(context.Background(), chatDBPath, limited)
	require.NoError(err)
	assertStoreCount(t, st.DB(), "messages", 1)

	summary, err := importer.Import(context.Background(), chatDBPath, appleTestOptions())
	require.NoError(err)
	assert.Equal(int64(3), summary.MessagesAdded)
	assertStoreCount(t, st.DB(), "messages", 4)
	assert.Equal("outbound prototype text", appleBodyText(t, st, "direct-out"))
}

// cancelAfterWrite cancels the import once a chat has written messages.
type cancelAfterWrite struct {
	NullProgress

	cancel context.CancelFunc
}

func (p cancelAfterWrite) OnChatComplete(_ string, added int64) {
	if added > 0 {
		p.cancel()
	}
}

func TestImportAppleRerunReadsChatsAfterInterruptedOtherDatabase(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	first := createAppleMarkerFixture(t)
	other := createAppleMarkerFixture(t)
	execAppleFixture(t, other, `
		UPDATE Z_METADATA SET Z_UUID = 'store-uuid-b';
		UPDATE ZWAMESSAGE SET ZTEXT = 'other ' || Z_PK WHERE ZTEXT IS NOT NULL;
	`)
	st := testutil.NewTestStore(t)
	_, err := NewImporter(st, nil).Import(context.Background(), first, appleTestOptions())
	require.NoError(err)
	ids := []string{"direct-in", "direct-out", "group-in", "lid-in"}
	original := make(map[string]string, len(ids))
	for _, id := range ids {
		original[id] = appleBodyText(t, st, id)
	}

	// Database B overwrites the first chat it writes, then is cancelled.
	ctx, cancel := context.WithCancel(context.Background())
	_, err = NewImporter(st, cancelAfterWrite{cancel: cancel}).Import(ctx, other, appleTestOptions())
	require.ErrorIs(err, context.Canceled)
	changed := 0
	for _, id := range ids {
		if appleBodyText(t, st, id) != original[id] {
			changed++
		}
	}
	require.Positive(changed, "the interrupted import must have written")
	markAppleMessagesUnwritten(t, st)

	_, err = NewImporter(st, nil).Import(context.Background(), first, appleTestOptions())
	require.NoError(err)
	assert.Len(rewrittenAppleMessages(t, st), changed)
	for _, id := range ids {
		assert.Equal(original[id], appleBodyText(t, st, id), id)
	}
}
