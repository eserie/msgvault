package matrix

import (
	"database/sql"
	"encoding/json/v2"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/store"
	"go.kenn.io/msgvault/internal/testutil"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

func TestImporterBackfillsJoinedRoomAndPersistsCheckpoint(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /_matrix/client/v3/sync", func(w http.ResponseWriter, r *http.Request) {
		assert.Equal("offline", r.URL.Query().Get("set_presence"),
			"every Matrix sync request must keep the archival account offline")
		_, _ = w.Write([]byte(`{"next_batch":"next-1","rooms":{"join":{"!room:example.org":{"state":{"events":[{"type":"m.room.name","state_key":"","content":{"name":"Example room"}}]},"timeline":{"events":[{"type":"m.room.message","event_id":"$edit","sender":"@member:example.org","origin_server_ts":3000,"content":{"msgtype":"m.text","body":"* updated","m.new_content":{"msgtype":"m.text","body":"updated"},"m.relates_to":{"rel_type":"m.replace","event_id":"$one"}}},{"type":"m.room.message","event_id":"$foreign-edit","sender":"@intruder:example.org","origin_server_ts":3500,"content":{"msgtype":"m.text","body":"* replaced by someone else","m.new_content":{"msgtype":"m.text","body":"replaced by someone else"},"m.relates_to":{"rel_type":"m.replace","event_id":"$one"}}},{"type":"m.reaction","event_id":"$reaction","sender":"@archive:example.org","origin_server_ts":4000,"content":{"m.relates_to":{"rel_type":"m.annotation","event_id":"$one","key":"👍"}}},{"type":"m.reaction","event_id":"$reaction-removed","sender":"@archive:example.org","origin_server_ts":4250,"content":{"m.relates_to":{"rel_type":"m.annotation","event_id":"$one","key":"👎"}}},{"type":"m.room.redaction","event_id":"$redact-reaction","sender":"@archive:example.org","origin_server_ts":4500,"redacts":"$reaction-removed","content":{}},{"type":"m.room.redaction","event_id":"$redact-message","sender":"@archive:example.org","origin_server_ts":4750,"redacts":"$delete-me","content":{}},{"type":"org.matrix.msc4075.rtc.notification","event_id":"$unsupported-call","sender":"@member:example.org","origin_server_ts":4900,"content":{"notification_type":"ring"}},{"type":"m.room.encrypted","event_id":"$encrypted","sender":"@member:example.org","origin_server_ts":5000,"content":{"algorithm":"m.megolm.v1.aes-sha2","ciphertext":"opaque","session_id":"session","sender_key":"key"}},{"type":"m.room.message","event_id":"$reply","sender":"@archive:example.org","origin_server_ts":5500,"content":{"msgtype":"m.text","body":"reply","m.relates_to":{"m.in_reply_to":{"event_id":"$one"}}}},{"type":"m.room.message","event_id":"$image","sender":"@member:example.org","origin_server_ts":6000,"content":{"msgtype":"m.image","body":"photo.jpg","url":"mxc://example.org/image","info":{"mimetype":"image/jpeg","size":1234}}},{"type":"m.room.message","event_id":"$orphan-reply","sender":"@member:example.org","origin_server_ts":6500,"content":{"msgtype":"m.text","body":"visible reply","m.relates_to":{"m.in_reply_to":{"event_id":"$not-visible"}}}}],"prev_batch":"older-1"}}}}}`))
	})
	mux.HandleFunc("GET /_matrix/client/v3/rooms/!room:example.org/messages", func(w http.ResponseWriter, r *http.Request) {
		assert.Equal("older-1", r.URL.Query().Get("from"))
		assert.Equal("b", r.URL.Query().Get("dir"))
		_, _ = w.Write([]byte(`{"chunk":[{"type":"m.room.message","event_id":"$delete-me","sender":"@member:example.org","origin_server_ts":1500,"content":{"msgtype":"m.text","body":"remove this"}},{"type":"m.room.message","event_id":"$one","sender":"@member:example.org","origin_server_ts":1000,"content":{"msgtype":"m.text","body":"original"}}],"end":""}`))
	})
	mux.HandleFunc("GET /_matrix/client/v3/user/@archive:example.org/account_data/m.direct", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"@member:example.org":["!room:example.org"]}`))
	})
	mux.HandleFunc("GET /_matrix/client/v3/rooms/!room:example.org/joined_members", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"joined":{"@archive:example.org":{"display_name":"Archive"},"@member:example.org":{"display_name":"Member"}}}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	client, err := mautrix.NewClient(server.URL, id.UserID("@archive:example.org"), "token")
	require.NoError(err)

	st := testutil.NewTestStore(t)
	source, err := st.GetOrCreateSource(SourceType, "@archive:example.org")
	require.NoError(err)
	sum, err := NewImporter(st, &Runtime{Client: client}).Import(t.Context(), ImportOptions{UserID: "@archive:example.org"})
	require.NoError(err)
	assert.Equal(int64(6), sum.MessagesAdded)
	assert.Equal(int64(1), sum.Undecryptable)
	assert.Equal(int64(1), sum.EventsSkipped)
	count, err := st.CountMessagesForSource(source.ID)
	require.NoError(err)
	assert.Equal(int64(5), count)
	messageIDs, err := st.MessageExistsBatch(source.ID, []string{"$one", "$delete-me", "$encrypted", "$reply", "$image", "$orphan-reply"})
	require.NoError(err)
	body, err := st.GetMessageBodyText(messageIDs["$one"])
	require.NoError(err)
	assert.Equal("updated", body)
	originalRaw, err := st.GetMessageRaw(messageIDs["$one"])
	require.NoError(err)
	var original event.Event
	require.NoError(json.Unmarshal(originalRaw, &original))
	require.NoError(original.Content.ParseRaw(original.Type))
	assert.Equal(id.EventID("$one"), original.ID)
	assert.Equal("original", original.Content.AsMessage().Body)
	body, err = st.GetMessageBodyText(messageIDs["$encrypted"])
	require.NoError(err)
	assert.Equal(encryptedPlaceholder, body)
	rawRows, err := st.ScanArchivedRawMessages(source.ID, rawFormat, 0, 10)
	require.NoError(err)
	var encrypted event.Event
	for _, row := range rawRows {
		var evt event.Event
		require.NoError(json.Unmarshal(row.RawData, &evt))
		if evt.ID == "$encrypted" {
			encrypted = evt
		}
	}
	assert.Equal(id.RoomID("!room:example.org"), encrypted.RoomID)
	var replyToMessageID int64
	require.NoError(st.DB().QueryRow(`SELECT reply_to_message_id FROM messages WHERE id = ?`, messageIDs["$reply"]).Scan(&replyToMessageID))
	assert.Equal(messageIDs["$one"], replyToMessageID)
	var orphanReplyTargetValid bool
	require.NoError(st.DB().QueryRow(`SELECT reply_to_message_id IS NOT NULL FROM messages WHERE id = ?`, messageIDs["$orphan-reply"]).Scan(&orphanReplyTargetValid))
	assert.False(orphanReplyTargetValid)
	var deletedAtValid bool
	require.NoError(st.DB().QueryRow(`SELECT deleted_from_source_at IS NOT NULL FROM messages WHERE id = ?`, messageIDs["$delete-me"]).Scan(&deletedAtValid))
	assert.True(deletedAtValid)
	var reactions int
	require.NoError(st.DB().QueryRow(`SELECT COUNT(*) FROM reactions WHERE message_id = ?`, messageIDs["$one"]).Scan(&reactions))
	assert.Equal(1, reactions)
	var sourceReactionID string
	require.NoError(st.DB().QueryRow(`
		SELECT rse.source_reaction_id
		FROM reaction_source_events rse
		JOIN reactions r ON r.id = rse.reaction_id
		WHERE r.message_id = ?`, messageIDs["$one"]).Scan(&sourceReactionID))
	assert.Equal("$reaction", sourceReactionID)
	assert.Equal(int64(1), sum.RelationsUnresolved)
	var conversationType string
	require.NoError(st.DB().QueryRow(`SELECT conversation_type FROM conversations WHERE source_id = ?`, source.ID).Scan(&conversationType))
	assert.Equal("direct_chat", conversationType)
	run, err := st.GetLastSuccessfulSync(source.ID)
	require.NoError(err)
	state, err := loadSyncState(run.CursorAfter.String)
	require.NoError(err)
	assert.Equal("next-1", state.NextBatch)
	assert.True(state.Rooms["!room:example.org"].Backfilled)

	fullSummary, err := NewImporter(st, &Runtime{Client: client}).Import(t.Context(), ImportOptions{
		UserID: "@archive:example.org", Full: true,
	})
	require.NoError(err)
	assert.Equal(int64(0), fullSummary.MessagesAdded)
	assert.Equal(int64(1), fullSummary.RelationsUnresolved, "replayed redactions stay resolved after their target identity is removed")
	body, err = st.GetMessageBodyText(messageIDs["$one"])
	require.NoError(err)
	assert.Equal("updated", body)
}

func TestImporterFullSyncIgnoresMalformedSavedCursor(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /_matrix/client/v3/sync", func(w http.ResponseWriter, r *http.Request) {
		assert.Empty(r.URL.Query().Get("since"))
		_, _ = w.Write([]byte(`{"next_batch":"fresh","rooms":{"join":{}}}`))
	})
	mux.HandleFunc("GET /_matrix/client/v3/user/@archive:example.org/account_data/m.direct", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	client, err := mautrix.NewClient(server.URL, id.UserID("@archive:example.org"), "token")
	require.NoError(err)

	st := testutil.NewTestStore(t)
	source, err := st.GetOrCreateSource(SourceType, "@archive:example.org")
	require.NoError(err)
	syncID, err := st.StartSync(source.ID, SourceType)
	require.NoError(err)
	require.NoError(st.CompleteSync(syncID, "malformed saved cursor"))

	_, err = NewImporter(st, &Runtime{Client: client}).Import(t.Context(), ImportOptions{
		UserID: source.Identifier, Full: true,
	})
	require.NoError(err)
}

func TestImporterDoesNotRecreateRemovedSource(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	st := testutil.NewTestStore(t)
	source, err := st.GetOrCreateSource(SourceType, "@archive:example.org")
	require.NoError(err)
	_, _, err = st.RemoveSourceSerialized(t.Context(), source.ID)
	require.NoError(err)
	client, err := mautrix.NewClient("https://example.invalid", id.UserID(source.Identifier), "token")
	require.NoError(err)

	_, err = NewImporter(st, &Runtime{Client: client}).Import(t.Context(), ImportOptions{
		UserID: source.Identifier,
	})

	require.ErrorIs(err, store.ErrSourceNotFound)
	sources, listErr := st.ListSources(SourceType)
	require.NoError(listErr)
	assert.Empty(sources)
}

func TestRoomIncluded(t *testing.T) {
	tests := []struct {
		name    string
		include []string
		exclude []string
		want    bool
	}{
		{name: "all rooms by default", want: true},
		{name: "included room", include: []string{"!room:example.org"}, want: true},
		{name: "not in include list", include: []string{"!other:example.org"}},
		{name: "excluded room", exclude: []string{"!room:example.org"}},
		{name: "exclude wins", include: []string{"!room:example.org"}, exclude: []string{"!room:example.org"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, roomIncluded("!room:example.org", tt.include, tt.exclude))
		})
	}
}

func TestRoomTitleUsesLatestTimelineRename(t *testing.T) {
	assert := assert.New(t)
	first := matrixTestEvent(t, `{"type":"m.room.name","event_id":"$name-1","state_key":"","content":{"name":"First"}}`)
	second := matrixTestEvent(t, `{"type":"m.room.name","event_id":"$name-2","state_key":"","content":{"name":"Second"}}`)
	title, present := roomTitle([]*event.Event{first, second})
	assert.True(present)
	assert.Equal("Second", title)
	empty, present := roomTitle([]*event.Event{matrixTestEvent(t, `{"type":"m.room.name","event_id":"$name-3","state_key":"","content":{"name":""}}`)})
	assert.True(present)
	assert.Empty(empty)
}

func TestImporterReclassifiesInactiveRoomFromDirectAccountData(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	var syncCalls int
	mux := http.NewServeMux()
	mux.HandleFunc("GET /_matrix/client/v3/sync", func(w http.ResponseWriter, _ *http.Request) {
		syncCalls++
		if syncCalls == 1 {
			_, _ = w.Write([]byte(`{"next_batch":"next-1","rooms":{"join":{"!room:example.org":{"state":{"events":[]},"timeline":{"events":[]}}}}}`))
			return
		}
		_, _ = w.Write([]byte(`{"next_batch":"next-2","rooms":{"join":{}}}`))
	})
	mux.HandleFunc("GET /_matrix/client/v3/user/@archive:example.org/account_data/m.direct", func(w http.ResponseWriter, _ *http.Request) {
		if syncCalls == 1 {
			_, _ = w.Write([]byte(`{}`))
			return
		}
		_, _ = w.Write([]byte(`{"@member:example.org":["!room:example.org"]}`))
	})
	mux.HandleFunc("GET /_matrix/client/v3/rooms/!room:example.org/joined_members", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"joined":{"@archive:example.org":{"display_name":"Archive"},"@member:example.org":{"display_name":"Member"}}}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	client, err := mautrix.NewClient(server.URL, id.UserID("@archive:example.org"), "token")
	require.NoError(err)

	st := testutil.NewTestStore(t)
	_, err = st.GetOrCreateSource(SourceType, "@archive:example.org")
	require.NoError(err)
	importer := NewImporter(st, &Runtime{Client: client})
	_, err = importer.Import(t.Context(), ImportOptions{UserID: "@archive:example.org"})
	require.NoError(err)
	_, err = NewImporter(st, &Runtime{Client: client}).Import(t.Context(), ImportOptions{UserID: "@archive:example.org"})
	require.NoError(err)
	var conversationType string
	require.NoError(st.DB().QueryRow(`SELECT conversation_type FROM conversations WHERE source_conversation_id = ?`, "!room:example.org").Scan(&conversationType))
	assert.Equal("direct_chat", conversationType)
}

func TestImporterExplicitEmptyRoomNameClearsArchivedTitle(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	var syncCalls int
	mux := http.NewServeMux()
	mux.HandleFunc("GET /_matrix/client/v3/sync", func(w http.ResponseWriter, _ *http.Request) {
		syncCalls++
		name := "Named room"
		if syncCalls > 1 {
			name = ""
		}
		_, _ = fmt.Fprintf(w, `{"next_batch":"next-%d","rooms":{"join":{"!room:example.org":{"state":{"events":[{"type":"m.room.name","state_key":"","content":{"name":%q}}]},"timeline":{"events":[]}}}}}`, syncCalls, name)
	})
	mux.HandleFunc("GET /_matrix/client/v3/user/@archive:example.org/account_data/m.direct", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{}`))
	})
	mux.HandleFunc("GET /_matrix/client/v3/rooms/!room:example.org/joined_members", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"joined":{"@archive:example.org":{"display_name":"Archive"},"@member:example.org":{"display_name":"Member fallback"}}}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	client, err := mautrix.NewClient(server.URL, id.UserID("@archive:example.org"), "token")
	require.NoError(err)

	st := testutil.NewTestStore(t)
	_, err = st.GetOrCreateSource(SourceType, "@archive:example.org")
	require.NoError(err)
	_, err = NewImporter(st, &Runtime{Client: client}).Import(t.Context(), ImportOptions{UserID: "@archive:example.org"})
	require.NoError(err)
	_, err = NewImporter(st, &Runtime{Client: client}).Import(t.Context(), ImportOptions{UserID: "@archive:example.org"})
	require.NoError(err)
	var title string
	require.NoError(st.DB().QueryRow(`SELECT title FROM conversations WHERE source_conversation_id = ?`, "!room:example.org").Scan(&title))
	assert.Empty(title)
}

func TestLatestEditWinsOriginalAtEqualTimestamp(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	st := testutil.NewTestStore(t)
	source, err := st.GetOrCreateSource(SourceType, "@archive:example.org")
	require.NoError(err)
	conversationID, err := st.EnsureConversationWithType(source.ID, "!room:example.org", "group_chat", "Example")
	require.NoError(err)
	client, err := mautrix.NewClient("https://example.invalid", id.UserID("@archive:example.org"), "token")
	require.NoError(err)
	imp := NewImporter(st, &Runtime{Client: client})
	sum := &ImportSummary{}
	require.NoError(imp.persistEvent(t.Context(), source.ID, conversationID,
		matrixTestEvent(t, `{"type":"m.room.message","event_id":"$z-original","sender":"@member:example.org","origin_server_ts":1000,"content":{"msgtype":"m.text","body":"original"}}`), sum))
	require.NoError(imp.persistEvent(t.Context(), source.ID, conversationID,
		matrixTestEvent(t, `{"type":"m.room.message","event_id":"$a-edit","sender":"@member:example.org","origin_server_ts":1000,"content":{"msgtype":"m.text","body":"* edited","m.new_content":{"msgtype":"m.text","body":"edited"},"m.relates_to":{"rel_type":"m.replace","event_id":"$z-original"}}}`), sum))
	messageIDs, err := st.MessageExistsBatch(source.ID, []string{"$z-original"})
	require.NoError(err)
	body, err := st.GetMessageBodyText(messageIDs["$z-original"])
	require.NoError(err)
	assert.Equal("edited", body)
}

func TestImporterSkipsFirstSeenStrippedMessage(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	st := testutil.NewTestStore(t)
	source, err := st.GetOrCreateSource(SourceType, "@archive:example.org")
	require.NoError(err)
	conversationID, err := st.EnsureConversationWithType(source.ID, "!room:example.org", "group_chat", "Example")
	require.NoError(err)
	client, err := mautrix.NewClient("https://example.invalid", id.UserID("@archive:example.org"), "token")
	require.NoError(err)
	imp := NewImporter(st, &Runtime{Client: client})
	require.NoError(imp.persistEvent(t.Context(), source.ID, conversationID,
		matrixTestEvent(t, `{"type":"m.room.message","event_id":"$already-redacted","sender":"@member:example.org","origin_server_ts":1000,"content":{},"unsigned":{"redacted_because":{"type":"m.room.redaction","event_id":"$redaction","sender":"@member:example.org","content":{}}}}`), &ImportSummary{}))
	messageIDs, err := st.MessageExistsBatch(source.ID, []string{"$already-redacted"})
	require.NoError(err)
	assert.Zero(messageIDs["$already-redacted"])
}

func TestImporterKeepsNewestEditAndRecomputesAfterRedaction(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	st := testutil.NewTestStore(t)
	source, err := st.GetOrCreateSource(SourceType, "@archive:example.org")
	require.NoError(err)
	conversationID, err := st.EnsureConversationWithType(source.ID, "!room:example.org", "group_chat", "Example")
	require.NoError(err)
	client, err := mautrix.NewClient("https://example.invalid", id.UserID("@archive:example.org"), "token")
	require.NoError(err)
	imp := NewImporter(st, &Runtime{Client: client})
	sum := &ImportSummary{}

	original := matrixTestEvent(t, `{"type":"m.room.message","event_id":"$original","sender":"@member:example.org","origin_server_ts":1000,"content":{"msgtype":"m.text","body":"original"}}`)
	newer := matrixTestEvent(t, `{"type":"m.room.message","event_id":"$newer","sender":"@member:example.org","origin_server_ts":3000,"content":{"msgtype":"m.text","body":"* newer version","m.new_content":{"msgtype":"m.text","body":"newer version"},"m.relates_to":{"rel_type":"m.replace","event_id":"$original"}}}`)
	older := matrixTestEvent(t, `{"type":"m.room.message","event_id":"$older","sender":"@member:example.org","origin_server_ts":2000,"content":{"msgtype":"m.text","body":"* old","m.new_content":{"msgtype":"m.text","body":"old"},"m.relates_to":{"rel_type":"m.replace","event_id":"$original"}}}`)
	redaction := matrixTestEvent(t, `{"type":"m.room.redaction","event_id":"$redact","sender":"@member:example.org","origin_server_ts":4000,"redacts":"$newer","content":{}}`)
	for _, evt := range []*event.Event{original, newer, older} {
		require.NoError(imp.persistEvent(t.Context(), source.ID, conversationID, evt, sum))
	}
	messageIDs, err := st.MessageExistsBatch(source.ID, []string{"$original"})
	require.NoError(err)
	body, err := st.GetMessageBodyText(messageIDs["$original"])
	require.NoError(err)
	assert.Equal("newer version", body, "an older edit from a later sync must not win")
	var sizeEstimate int64
	require.NoError(st.DB().QueryRow(`SELECT size_estimate FROM messages WHERE id = ?`, messageIDs["$original"]).Scan(&sizeEstimate))
	assert.Equal(int64(len("newer version")), sizeEstimate)
	labelID, err := st.EnsureLabel(source.ID, "local-review", "Local review", "user")
	require.NoError(err)
	require.NoError(st.AddMessageLabels(messageIDs["$original"], []int64{labelID}))
	_, err = st.DB().Exec(`UPDATE messages SET embed_gen = 7 WHERE id = ?`, messageIDs["$original"])
	require.NoError(err)
	require.NoError(imp.persistEvent(t.Context(), source.ID, conversationID, original, sum))
	var embedGen int64
	require.NoError(st.DB().QueryRow(`SELECT embed_gen FROM messages WHERE id = ?`, messageIDs["$original"]).Scan(&embedGen))
	assert.Equal(int64(7), embedGen, "unchanged full replay must preserve the selected edit's embedding generation")
	labelIDs, err := st.MessageLabelIDsContext(t.Context(), messageIDs["$original"])
	require.NoError(err)
	assert.Equal([]int64{labelID}, labelIDs, "provider replay must preserve locally assigned labels")
	require.NoError(imp.persistEvent(t.Context(), source.ID, conversationID, redaction, sum))
	body, err = st.GetMessageBodyText(messageIDs["$original"])
	require.NoError(err)
	assert.Equal("old", body, "redacting the winning edit selects the latest survivor")
	require.NoError(st.DB().QueryRow(`SELECT size_estimate FROM messages WHERE id = ?`, messageIDs["$original"]).Scan(&sizeEstimate))
	assert.Equal(int64(len("old")), sizeEstimate)
}

func TestImporterRejectsCrossRoomRelations(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	st := testutil.NewTestStore(t)
	source, err := st.GetOrCreateSource(SourceType, "@archive:example.org")
	require.NoError(err)
	roomA, err := st.EnsureConversationWithType(source.ID, "!room-a:example.org", "group_chat", "Room A")
	require.NoError(err)
	roomB, err := st.EnsureConversationWithType(source.ID, "!room-b:example.org", "group_chat", "Room B")
	require.NoError(err)
	client, err := mautrix.NewClient("https://example.invalid", id.UserID("@archive:example.org"), "token")
	require.NoError(err)
	imp := NewImporter(st, &Runtime{Client: client})
	sum := &ImportSummary{}
	original := matrixTestEvent(t, `{"type":"m.room.message","event_id":"$room-a-message","sender":"@member:example.org","origin_server_ts":1000,"content":{"msgtype":"m.text","body":"room A"}}`)
	edit := matrixTestEvent(t, `{"type":"m.room.message","event_id":"$room-a-edit","sender":"@member:example.org","origin_server_ts":2000,"content":{"msgtype":"m.text","body":"* edited","m.new_content":{"msgtype":"m.text","body":"edited"},"m.relates_to":{"rel_type":"m.replace","event_id":"$room-a-message"}}}`)
	reaction := matrixTestEvent(t, `{"type":"m.reaction","event_id":"$room-a-reaction","sender":"@member:example.org","origin_server_ts":3000,"content":{"m.relates_to":{"rel_type":"m.annotation","event_id":"$room-a-message","key":"ok"}}}`)
	for _, evt := range []*event.Event{original, edit, reaction} {
		require.NoError(imp.persistEvent(t.Context(), source.ID, roomA, evt, sum))
	}
	crossRoomReaction := matrixTestEvent(t, `{"type":"m.reaction","event_id":"$room-b-reaction","sender":"@member:example.org","origin_server_ts":4000,"content":{"m.relates_to":{"rel_type":"m.annotation","event_id":"$room-a-message","key":"no"}}}`)
	require.NoError(imp.persistEvent(t.Context(), source.ID, roomB, crossRoomReaction, sum))
	for i, target := range []string{"$room-a-message", "$room-a-edit", "$room-a-reaction"} {
		redaction := matrixTestEvent(t, fmt.Sprintf(`{"type":"m.room.redaction","event_id":"$room-b-redaction-%d","sender":"@member:example.org","origin_server_ts":%d,"redacts":%q,"content":{}}`, i, 5000+i, target))
		require.NoError(imp.persistEvent(t.Context(), source.ID, roomB, redaction, sum))
	}
	var reactions int
	require.NoError(st.DB().QueryRow(`SELECT COUNT(*) FROM reactions`).Scan(&reactions))
	assert.Equal(1, reactions)
	messageIDs, err := st.MessageExistsBatch(source.ID, []string{"$room-a-message"})
	require.NoError(err)
	body, err := st.GetMessageBodyText(messageIDs["$room-a-message"])
	require.NoError(err)
	assert.Equal("edited", body)
	var deleted bool
	require.NoError(st.DB().QueryRow(`SELECT deleted_from_source_at IS NOT NULL FROM messages WHERE id = ?`,
		messageIDs["$room-a-message"]).Scan(&deleted))
	assert.False(deleted)
	var redactedMarkers int
	require.NoError(st.DB().QueryRow(`SELECT COUNT(*) FROM matrix_redacted_events
		WHERE source_id = ?`, source.ID).Scan(&redactedMarkers))
	assert.Zero(redactedMarkers)
}

func TestImporterDoesNotRestoreRedactedRelationPayloads(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	st := testutil.NewTestStore(t)
	source, err := st.GetOrCreateSource(SourceType, "@archive:example.org")
	require.NoError(err)
	conversationID, err := st.EnsureConversationWithType(source.ID, "!room:example.org", "group_chat", "Example")
	require.NoError(err)
	client, err := mautrix.NewClient("https://example.invalid", id.UserID("@archive:example.org"), "token")
	require.NoError(err)
	imp := NewImporter(st, &Runtime{Client: client})
	sum := &ImportSummary{}
	original := matrixTestEvent(t, `{"type":"m.room.message","event_id":"$original","sender":"@member:example.org","origin_server_ts":1000,"content":{"msgtype":"m.text","body":"original"}}`)
	reaction := matrixTestEvent(t, `{"type":"m.reaction","event_id":"$reaction","sender":"@member:example.org","origin_server_ts":2000,"content":{"m.relates_to":{"rel_type":"m.annotation","event_id":"$original","key":"ok"}}}`)
	edit := matrixTestEvent(t, `{"type":"m.room.message","event_id":"$edit","sender":"@member:example.org","origin_server_ts":3000,"content":{"msgtype":"m.text","body":"* edited","m.new_content":{"msgtype":"m.text","body":"edited"},"m.relates_to":{"rel_type":"m.replace","event_id":"$original"}}}`)
	for _, evt := range []*event.Event{original, reaction, edit} {
		require.NoError(imp.persistEvent(t.Context(), source.ID, conversationID, evt, sum))
	}
	for _, target := range []string{"$reaction", "$edit"} {
		redaction := matrixTestEvent(t, `{"type":"m.room.redaction","event_id":"$redact","sender":"@member:example.org","origin_server_ts":4000,"redacts":"`+target+`","content":{}}`)
		require.NoError(imp.persistEvent(t.Context(), source.ID, conversationID, redaction, sum))
	}
	require.NoError(imp.persistEvent(t.Context(), source.ID, conversationID, reaction, sum))
	require.NoError(imp.persistEvent(t.Context(), source.ID, conversationID, edit, sum))
	var reactions int
	require.NoError(st.DB().QueryRow(`SELECT COUNT(*) FROM reactions`).Scan(&reactions))
	assert.Zero(reactions)
	messageIDs, err := st.MessageExistsBatch(source.ID, []string{"$original"})
	require.NoError(err)
	body, err := st.GetMessageBodyText(messageIDs["$original"])
	require.NoError(err)
	assert.Equal("original", body)
}

func TestImporterFullReplayPreservesRedactedMessageContent(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	st := testutil.NewTestStore(t)
	source, err := st.GetOrCreateSource(SourceType, "@archive:example.org")
	require.NoError(err)
	conversationID, err := st.EnsureConversationWithType(source.ID, "!room:example.org", "group_chat", "Example")
	require.NoError(err)
	client, err := mautrix.NewClient("https://example.invalid", id.UserID("@archive:example.org"), "token")
	require.NoError(err)
	imp := NewImporter(st, &Runtime{Client: client})
	sum := &ImportSummary{}
	require.NoError(imp.persistEvent(t.Context(), source.ID, conversationID,
		matrixTestEvent(t, `{"type":"m.room.message","event_id":"$one","sender":"@member:example.org","origin_server_ts":1000,"content":{"msgtype":"m.text","body":"retained"}}`), sum))
	require.NoError(imp.persistEvent(t.Context(), source.ID, conversationID,
		matrixTestEvent(t, `{"type":"m.room.message","event_id":"$one","sender":"@member:example.org","origin_server_ts":1000,"content":{},"unsigned":{"redacted_because":{"type":"m.room.redaction","event_id":"$redaction","sender":"@member:example.org","content":{}}}}`), sum))
	messageIDs, err := st.MessageExistsBatch(source.ID, []string{"$one"})
	require.NoError(err)
	body, err := st.GetMessageBodyText(messageIDs["$one"])
	require.NoError(err)
	assert.Equal("retained", body)
	var deleted bool
	require.NoError(st.DB().QueryRow(`SELECT deleted_from_source_at IS NOT NULL FROM messages WHERE id = ?`, messageIDs["$one"]).Scan(&deleted))
	assert.True(deleted)
}

func TestImporterAppliesTargetOfStrippedRedaction(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	st := testutil.NewTestStore(t)
	source, err := st.GetOrCreateSource(SourceType, "@archive:example.org")
	require.NoError(err)
	conversationID, err := st.EnsureConversationWithType(source.ID, "!room:example.org", "group_chat", "Example")
	require.NoError(err)
	client, err := mautrix.NewClient("https://example.invalid", "@archive:example.org", "token")
	require.NoError(err)
	imp := NewImporter(st, &Runtime{Client: client})
	sum := &ImportSummary{}
	require.NoError(imp.persistEvent(t.Context(), source.ID, conversationID,
		matrixTestEvent(t, `{"type":"m.room.message","event_id":"$message","sender":"@member:example.org","origin_server_ts":1000,"content":{"msgtype":"m.text","body":"retained"}}`), sum))
	require.NoError(imp.persistEvent(t.Context(), source.ID, conversationID,
		matrixTestEvent(t, `{"type":"m.room.redaction","event_id":"$redaction","sender":"@member:example.org","origin_server_ts":2000,"content":{"redacts":"$message"},"unsigned":{"redacted_because":{"type":"m.room.redaction","event_id":"$redact-redaction","sender":"@member:example.org","content":{}}}}`), sum))
	messageIDs, err := st.MessageExistsBatch(source.ID, []string{"$message"})
	require.NoError(err)
	var deleted bool
	require.NoError(st.DB().QueryRow(`SELECT deleted_from_source_at IS NOT NULL FROM messages WHERE id = ?`, messageIDs["$message"]).Scan(&deleted))
	assert.True(deleted)
}

func TestImporterSeedsLegacyOriginalBeforeEditRedaction(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	st := testutil.NewTestStore(t)
	source, err := st.GetOrCreateSource(SourceType, "@archive:example.org")
	require.NoError(err)
	conversationID, err := st.EnsureConversationWithType(source.ID, "!room:example.org", "group_chat", "Example")
	require.NoError(err)
	client, err := mautrix.NewClient("https://example.invalid", id.UserID("@archive:example.org"), "token")
	require.NoError(err)
	imp := NewImporter(st, &Runtime{Client: client})
	sum := &ImportSummary{}
	original := matrixTestEvent(t, `{"type":"m.room.message","event_id":"$legacy","sender":"@member:example.org","origin_server_ts":1000,"content":{"msgtype":"m.text","body":"legacy original"}}`)
	edit := matrixTestEvent(t, `{"type":"m.room.message","event_id":"$legacy-edit","sender":"@member:example.org","origin_server_ts":2000,"content":{"msgtype":"m.text","body":"* edited","m.new_content":{"msgtype":"m.text","body":"edited"},"m.relates_to":{"rel_type":"m.replace","event_id":"$legacy"}}}`)
	redaction := matrixTestEvent(t, `{"type":"m.room.redaction","event_id":"$redact-legacy-edit","sender":"@member:example.org","origin_server_ts":3000,"redacts":"$legacy-edit","content":{}}`)
	require.NoError(imp.persistEvent(t.Context(), source.ID, conversationID, original, sum))
	_, err = st.DB().Exec(`DELETE FROM matrix_message_versions`)
	require.NoError(err)
	require.NoError(imp.persistEvent(t.Context(), source.ID, conversationID, edit, sum))
	require.NoError(imp.persistEvent(t.Context(), source.ID, conversationID, redaction, sum))
	messageIDs, err := st.MessageExistsBatch(source.ID, []string{"$legacy"})
	require.NoError(err)
	body, err := st.GetMessageBodyText(messageIDs["$legacy"])
	require.NoError(err)
	assert.Equal("legacy original", body)
	var edited bool
	require.NoError(st.DB().QueryRow(`SELECT is_edited FROM messages WHERE id = ?`, messageIDs["$legacy"]).Scan(&edited))
	assert.False(edited)
}

func TestImporterFullReplayRedactedEditDoesNotCreateMessage(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	st := testutil.NewTestStore(t)
	source, err := st.GetOrCreateSource(SourceType, "@archive:example.org")
	require.NoError(err)
	conversationID, err := st.EnsureConversationWithType(source.ID, "!room:example.org", "group_chat", "Example")
	require.NoError(err)
	client, err := mautrix.NewClient("https://example.invalid", id.UserID("@archive:example.org"), "token")
	require.NoError(err)
	imp := NewImporter(st, &Runtime{Client: client})
	sum := &ImportSummary{}
	require.NoError(imp.persistEvent(t.Context(), source.ID, conversationID,
		matrixTestEvent(t, `{"type":"m.room.message","event_id":"$original-edit-target","sender":"@member:example.org","origin_server_ts":1000,"content":{"msgtype":"m.text","body":"original"}}`), sum))
	require.NoError(imp.persistEvent(t.Context(), source.ID, conversationID,
		matrixTestEvent(t, `{"type":"m.room.message","event_id":"$stripped-edit","sender":"@member:example.org","origin_server_ts":2000,"content":{"msgtype":"m.text","body":"* edited","m.new_content":{"msgtype":"m.text","body":"edited"},"m.relates_to":{"rel_type":"m.replace","event_id":"$original-edit-target"}}}`), sum))
	require.NoError(imp.persistEvent(t.Context(), source.ID, conversationID,
		matrixTestEvent(t, `{"type":"m.room.message","event_id":"$stripped-edit","sender":"@member:example.org","origin_server_ts":2000,"content":{},"unsigned":{"redacted_because":{"type":"m.room.redaction","event_id":"$redaction","sender":"@member:example.org","content":{}}}}`), sum))
	messageIDs, err := st.MessageExistsBatch(source.ID, []string{"$original-edit-target", "$stripped-edit"})
	require.NoError(err)
	assert.Zero(messageIDs["$stripped-edit"])
	body, err := st.GetMessageBodyText(messageIDs["$original-edit-target"])
	require.NoError(err)
	assert.Equal("original", body)
}

func matrixTestEvent(t *testing.T, raw string) *event.Event {
	t.Helper()
	var evt event.Event
	require.NoError(t, json.Unmarshal([]byte(raw), &evt))
	evt.RoomID = "!room:example.org"
	return &evt
}

func TestMessageBodyStripsMatrixReplyFallback(t *testing.T) {
	assert := assert.New(t)
	t.Parallel()
	content := &event.MessageEventContent{
		MsgType:       event.MsgText,
		Body:          "> <@other:example.org> quoted text\n>\nreply text",
		Format:        event.FormatHTML,
		FormattedBody: "<mx-reply><blockquote>quoted text</blockquote></mx-reply><p>reply text</p>",
		RelatesTo:     &event.RelatesTo{InReplyTo: &event.InReplyTo{EventID: "$target"}},
	}
	originalBody := content.Body
	originalFormattedBody := content.FormattedBody

	body := messageBody(content)

	assert.Equal("reply text", body)
	assert.NotContains(body, "quoted text")
	assert.Equal(originalBody, content.Body, "raw event content must remain unchanged")
	assert.Equal(originalFormattedBody, content.FormattedBody,
		"raw formatted event content must remain unchanged")
}

func TestOnlyMutatingMessageRelationsAreDeferred(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	reply := &event.Event{Type: event.EventMessage, Content: event.Content{Parsed: &event.MessageEventContent{
		MsgType:   event.MsgText,
		RelatesTo: &event.RelatesTo{InReplyTo: &event.InReplyTo{EventID: "$target"}},
	}}}
	deferred, err := shouldDeferRelation(reply)
	require.NoError(err)
	assert.False(deferred, "reply messages must exist before reactions are replayed")

	edit := &event.Event{Type: event.EventMessage, Content: event.Content{Parsed: &event.MessageEventContent{
		MsgType:   event.MsgText,
		RelatesTo: &event.RelatesTo{Type: event.RelReplace, EventID: "$target"},
	}}}
	deferred, err = shouldDeferRelation(edit)
	require.NoError(err)
	assert.True(deferred)
}

func TestDeferredReplyResolutionDoesNotOverwriteEditedBody(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	st := testutil.NewTestStore(t)
	source, err := st.GetOrCreateSource(SourceType, "@archive:example.org")
	require.NoError(err)
	conversationID, err := st.EnsureConversation(source.ID, "!room:example.org", "Example room")
	require.NoError(err)
	targetID, err := st.UpsertMessage(&store.Message{
		ConversationID: conversationID, SourceID: source.ID,
		SourceMessageID: "$target", MessageType: SourceType,
	})
	require.NoError(err)
	replyID, err := st.PersistMessage(&store.MessagePersistData{
		Message: &store.Message{
			ConversationID: conversationID, SourceID: source.ID,
			SourceMessageID: "$reply", MessageType: SourceType,
		},
		BodyText: sql.NullString{String: "newest edit", Valid: true},
	})
	require.NoError(err)
	reply := &event.Event{
		ID: "$reply", Type: event.EventMessage,
		Content: event.Content{Parsed: &event.MessageEventContent{
			MsgType: event.MsgText, Body: "original reply",
			RelatesTo: &event.RelatesTo{InReplyTo: &event.InReplyTo{EventID: "$target"}},
		}},
	}
	rawReply, err := json.Marshal(reply, json.Deterministic(true))
	require.NoError(err)
	var restoredReply event.Event
	require.NoError(json.Unmarshal(rawReply, &restoredReply))
	assert.Nil(restoredReply.Content.Parsed)

	require.NoError(NewImporter(st, nil).replayDeferredRelation(
		t.Context(), source.ID, conversationID, &restoredReply, &ImportSummary{},
	))
	body, err := st.GetMessageBodyText(replyID)
	require.NoError(err)
	assert.Equal("newest edit", body)
	var replyTo int64
	require.NoError(st.DB().QueryRow(`SELECT reply_to_message_id FROM messages WHERE id = ?`, replyID).Scan(&replyTo))
	assert.Equal(targetID, replyTo)
}

func TestImporterResumesFailedRoomBackfillCheckpoint(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	var requestedFrom []string
	failedOnce := false
	var syncCalls int
	mux := http.NewServeMux()
	mux.HandleFunc("GET /_matrix/client/v3/sync", func(w http.ResponseWriter, _ *http.Request) {
		syncCalls++
		if syncCalls == 1 {
			_, _ = w.Write([]byte(`{"next_batch":"next-1","rooms":{"join":{"!room:example.org":{"state":{"events":[]},"timeline":{"events":[{"type":"m.room.message","event_id":"$recent","sender":"@member:example.org","origin_server_ts":3000,"content":{"msgtype":"m.text","body":"recent"}},{"type":"m.room.message","event_id":"$edit-oldest","sender":"@member:example.org","origin_server_ts":3500,"content":{"msgtype":"m.text","body":"* edited","m.new_content":{"msgtype":"m.text","body":"edited"},"m.relates_to":{"rel_type":"m.replace","event_id":"$oldest"}}}],"prev_batch":"older-1"}}}}}`))
			return
		}
		_, _ = w.Write([]byte(`{"next_batch":"next-2","rooms":{"join":{"!room:example.org":{"state":{"events":[]},"timeline":{"events":[{"type":"m.room.message","event_id":"$new","sender":"@member:example.org","origin_server_ts":5000,"content":{"msgtype":"m.text","body":"new"}}],"prev_batch":"fresh-gap"}}}}}`))
	})
	mux.HandleFunc("GET /_matrix/client/v3/user/@archive:example.org/account_data/m.direct", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{}`))
	})
	mux.HandleFunc("GET /_matrix/client/v3/rooms/!room:example.org/joined_members", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"joined":{"@archive:example.org":{"display_name":"Archive"},"@member:example.org":{"display_name":"Member"}}}`))
	})
	mux.HandleFunc("GET /_matrix/client/v3/rooms/!room:example.org/messages", func(w http.ResponseWriter, r *http.Request) {
		from := r.URL.Query().Get("from")
		requestedFrom = append(requestedFrom, from)
		if from == "fresh-gap" {
			_, _ = w.Write([]byte(`{"chunk":[{"type":"m.room.message","event_id":"$between","sender":"@member:example.org","origin_server_ts":4000,"content":{"msgtype":"m.text","body":"between"}},{"type":"m.room.message","event_id":"$recent","sender":"@member:example.org","origin_server_ts":3000,"content":{"msgtype":"m.text","body":"recent"}},{"type":"m.room.message","event_id":"$edit-oldest","sender":"@member:example.org","origin_server_ts":3500,"content":{},"unsigned":{"redacted_because":{"type":"m.room.redaction","event_id":"$redact-edit","sender":"@member:example.org","content":{}}}}],"end":"older-1"}`))
			return
		}
		if from == "older-1" {
			_, _ = w.Write([]byte(`{"chunk":[{"type":"m.room.message","event_id":"$middle","sender":"@member:example.org","origin_server_ts":2000,"content":{"msgtype":"m.text","body":"middle"}}],"end":"older-2"}`))
			return
		}
		if !failedOnce {
			failedOnce = true
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"errcode":"M_FORBIDDEN","error":"temporary test interruption"}`))
			return
		}
		_, _ = w.Write([]byte(`{"chunk":[{"type":"m.room.message","event_id":"$oldest","sender":"@member:example.org","origin_server_ts":1000,"content":{"msgtype":"m.text","body":"oldest"}}],"end":""}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	client, err := mautrix.NewClient(server.URL, id.UserID("@archive:example.org"), "token")
	require.NoError(err)

	st := testutil.NewTestStore(t)
	source, err := st.GetOrCreateSource(SourceType, "@archive:example.org")
	require.NoError(err)
	options := ImportOptions{UserID: "@archive:example.org"}
	_, err = NewImporter(st, &Runtime{Client: client}).Import(t.Context(), options)
	require.Error(err)
	_, err = NewImporter(st, &Runtime{Client: client}).Import(t.Context(), options)
	require.NoError(err)
	require.Equal([]string{"older-1", "older-2", "fresh-gap", "older-2"}, requestedFrom)

	count, err := st.CountMessagesForSource(source.ID)
	require.NoError(err)
	assert.Equal(int64(5), count)
	messages, err := st.MessageExistsBatch(source.ID, []string{"$oldest"})
	require.NoError(err)
	body, err := st.GetMessageBodyText(messages["$oldest"])
	require.NoError(err)
	assert.Equal("oldest", body, "a stripped gap boundary removes the checkpointed edit before deferred replay")
}

func TestImporterAppliesBackfillEditsOldestFirst(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /_matrix/client/v3/sync", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"next_batch":"next","rooms":{"join":{"!room:example.org":{"state":{"events":[]},"timeline":{"events":[],"prev_batch":"newer-page"}}}}}`))
	})
	mux.HandleFunc("GET /_matrix/client/v3/user/@archive:example.org/account_data/m.direct", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{}`))
	})
	mux.HandleFunc("GET /_matrix/client/v3/rooms/!room:example.org/joined_members", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"joined":{"@member:example.org":{"display_name":"Member"}}}`))
	})
	mux.HandleFunc("GET /_matrix/client/v3/rooms/!room:example.org/messages", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("from") == "newer-page" {
			_, _ = w.Write([]byte(`{"chunk":[],"end":"middle-page"}`))
			return
		}
		if r.URL.Query().Get("from") == "middle-page" {
			_, _ = w.Write([]byte(`{"chunk":[{"type":"m.room.message","event_id":"$latest-edit","sender":"@member:example.org","origin_server_ts":3000,"content":{"msgtype":"m.text","body":"* latest","m.new_content":{"msgtype":"m.text","body":"latest"},"m.relates_to":{"rel_type":"m.replace","event_id":"$original"}}}],"end":"older-page"}`))
			return
		}
		_, _ = w.Write([]byte(`{"chunk":[{"type":"m.room.message","event_id":"$older-edit","sender":"@member:example.org","origin_server_ts":2000,"content":{"msgtype":"m.text","body":"* older","m.new_content":{"msgtype":"m.text","body":"older"},"m.relates_to":{"rel_type":"m.replace","event_id":"$original"}}},{"type":"m.room.message","event_id":"$original","sender":"@member:example.org","origin_server_ts":1000,"content":{"msgtype":"m.text","body":"original"}}],"end":""}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	client, err := mautrix.NewClient(server.URL, id.UserID("@archive:example.org"), "token")
	require.NoError(err)

	st := testutil.NewTestStore(t)
	source, err := st.GetOrCreateSource(SourceType, "@archive:example.org")
	require.NoError(err)
	_, err = NewImporter(st, &Runtime{Client: client}).Import(t.Context(), ImportOptions{UserID: "@archive:example.org"})
	require.NoError(err)
	messages, err := st.MessageExistsBatch(source.ID, []string{"$original"})
	require.NoError(err)
	body, err := st.GetMessageBodyText(messages["$original"])
	require.NoError(err)
	assert.Equal("latest", body)
}

func TestImporterFillsLimitedIncrementalTimelineGap(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	var gapRequests int
	mux := http.NewServeMux()
	mux.HandleFunc("GET /_matrix/client/v3/sync", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("since") == "next-2" {
			_, _ = w.Write([]byte(`{"next_batch":"next-3","rooms":{"join":{"!room:example.org":{"state":{"events":[]},"timeline":{"limited":true,"events":[{"type":"m.room.message","event_id":"$new","sender":"@member:example.org","origin_server_ts":3000,"content":{"msgtype":"m.text","body":"new"}}],"prev_batch":"gap-1"}}}}}`))
			return
		}
		if r.URL.Query().Get("since") == "next-1" {
			_, _ = w.Write([]byte(`{"next_batch":"next-2","rooms":{"join":{"!room:example.org":{"state":{"events":[]},"timeline":{"events":[],"prev_batch":""}}}}}`))
			return
		}
		_, _ = w.Write([]byte(`{"next_batch":"next-1","rooms":{"join":{"!room:example.org":{"state":{"events":[]},"timeline":{"events":[{"type":"m.room.message","event_id":"$known","sender":"@member:example.org","origin_server_ts":1000,"content":{"msgtype":"m.text","body":"known"}}],"prev_batch":"initial-backfill"}}}}}`))
	})
	mux.HandleFunc("GET /_matrix/client/v3/user/@archive:example.org/account_data/m.direct", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{}`))
	})
	mux.HandleFunc("GET /_matrix/client/v3/rooms/!room:example.org/joined_members", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"joined":{"@archive:example.org":{"display_name":"Archive"},"@member:example.org":{"display_name":"Member"}}}`))
	})
	mux.HandleFunc("GET /_matrix/client/v3/rooms/!room:example.org/messages", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("from") == "initial-backfill" {
			_, _ = w.Write([]byte(`{"chunk":[{"type":"m.room.message","event_id":"$stale","sender":"@member:example.org","origin_server_ts":1500,"content":{"msgtype":"m.text","body":"stale retry marker"}}],"end":""}`))
			return
		}
		gapRequests++
		if r.URL.Query().Get("from") == "gap-1" {
			_, _ = w.Write([]byte(`{"chunk":[],"end":"gap-2"}`))
			return
		}
		assert.Equal("gap-2", r.URL.Query().Get("from"))
		_, _ = w.Write([]byte(`{"chunk":[{"type":"m.room.message","event_id":"$stale","sender":"@member:example.org","origin_server_ts":2500,"content":{"msgtype":"m.text","body":"stale retry marker"}},{"type":"m.room.message","event_id":"$gap","sender":"@member:example.org","origin_server_ts":2000,"content":{"msgtype":"m.text","body":"gap"}},{"type":"m.room.message","event_id":"$known","sender":"@member:example.org","origin_server_ts":1000,"content":{"msgtype":"m.text","body":"known"}}],"end":"older"}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	client, err := mautrix.NewClient(server.URL, id.UserID("@archive:example.org"), "token")
	require.NoError(err)

	st := testutil.NewTestStore(t)
	source, err := st.GetOrCreateSource(SourceType, "@archive:example.org")
	require.NoError(err)
	opts := ImportOptions{UserID: "@archive:example.org"}
	_, err = NewImporter(st, &Runtime{Client: client}).Import(t.Context(), opts)
	require.NoError(err)
	_, err = NewImporter(st, &Runtime{Client: client}).Import(t.Context(), opts)
	require.NoError(err)
	third, err := NewImporter(st, &Runtime{Client: client}).Import(t.Context(), opts)
	require.NoError(err)
	assert.Equal(int64(2), third.MessagesAdded)
	assert.Equal(2, gapRequests)

	count, err := st.CountMessagesForSource(source.ID)
	require.NoError(err)
	assert.Equal(int64(4), count)
}

func TestImporterResumesPersistedGapBoundary(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	var gapRequests []string
	mux := http.NewServeMux()
	mux.HandleFunc("GET /_matrix/client/v3/sync", func(w http.ResponseWriter, r *http.Request) {
		assert.Equal("checkpoint", r.URL.Query().Get("since"))
		_, _ = w.Write([]byte(`{"next_batch":"next","rooms":{"join":{"!room:example.org":{"state":{"events":[]},"timeline":{"limited":true,"events":[{"type":"m.room.message","event_id":"$new-boundary","sender":"@member:example.org","origin_server_ts":3000,"content":{"msgtype":"m.text","body":"new boundary"}}],"prev_batch":"new-gap"}}}}}`))
	})
	mux.HandleFunc("GET /_matrix/client/v3/user/@archive:example.org/account_data/m.direct", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{}`))
	})
	mux.HandleFunc("GET /_matrix/client/v3/rooms/!room:example.org/joined_members", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"joined":{"@archive:example.org":{"display_name":"Archive"},"@member:example.org":{"display_name":"Member"}}}`))
	})
	mux.HandleFunc("GET /_matrix/client/v3/rooms/!room:example.org/messages", func(w http.ResponseWriter, r *http.Request) {
		from := r.URL.Query().Get("from")
		gapRequests = append(gapRequests, from)
		switch from {
		case "resume-gap":
			_, _ = w.Write([]byte(`{"chunk":[{"type":"m.reaction","event_id":"$relation-boundary","sender":"@member:example.org","origin_server_ts":2000,"content":{"m.relates_to":{"rel_type":"m.annotation","event_id":"$known","key":"thumbs"}}}],"end":"older"}`))
		default:
			http.Error(w, "unexpected gap cursor", http.StatusBadRequest)
		}
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	client, err := mautrix.NewClient(server.URL, id.UserID("@archive:example.org"), "token")
	require.NoError(err)

	st := testutil.NewTestStore(t)
	source, err := st.GetOrCreateSource(SourceType, "@archive:example.org")
	require.NoError(err)
	state := newSyncState()
	state.NextBatch = "checkpoint"
	state.Rooms["!room:example.org"] = &RoomState{
		Backfilled:            true,
		BoundaryEventIDs:      []string{"$known"},
		GapBatch:              "resume-gap",
		GapBoundaryReached:    true,
		GapPendingRelationIDs: []string{"$relation-boundary"},
		GapBoundaryIDs:        []string{"$relation-boundary"},
		PendingBoundaryIDs:    []string{"$new-boundary"},
	}
	cursor, err := state.marshal()
	require.NoError(err)
	syncID, err := st.StartSync(source.ID, SourceType)
	require.NoError(err)
	require.NoError(st.CompleteSync(syncID, cursor))

	_, err = NewImporter(st, &Runtime{Client: client}).Import(t.Context(), ImportOptions{UserID: "@archive:example.org"})
	require.NoError(err)
	assert.Equal([]string{"resume-gap"}, gapRequests,
		"resume must finish at the persisted remaining relation boundary")
	run, err := st.GetLastSuccessfulSync(source.ID)
	require.NoError(err)
	resumed, err := loadSyncState(run.CursorAfter.String)
	require.NoError(err)
	room := resumed.Rooms["!room:example.org"]
	require.NotNil(room)
	assert.True(room.Backfilled)
	assert.Equal([]string{"$new-boundary"}, room.BoundaryEventIDs)
	assert.Empty(room.GapBatch)
	assert.False(room.GapBoundaryReached)
	assert.Empty(room.GapPendingRelationIDs)
	assert.Empty(room.GapBoundaryIDs)
	assert.Empty(room.PendingBoundaryIDs)
}
