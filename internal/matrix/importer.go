package matrix

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json/v2"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	msgmime "go.kenn.io/msgvault/internal/mime"
	"go.kenn.io/msgvault/internal/store"
	"go.kenn.io/msgvault/internal/textutil"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

const (
	SourceType           = "matrix"
	rawFormat            = "matrix_json"
	encryptedPlaceholder = "[encrypted message — keys unavailable]"
	pageSize             = 100
)

var errRelationTargetMissing = errors.New("matrix relation target is not archived yet")

type RoomState struct {
	Backfilled            bool     `json:"backfilled,omitzero"`
	PrevBatch             string   `json:"prev_batch,omitempty"`
	GapBatch              string   `json:"gap_batch,omitempty"`
	LegacyGapBridge       bool     `json:"legacy_gap_bridge,omitzero"`
	GapBoundaryReached    bool     `json:"gap_boundary_reached,omitzero"`
	GapPendingRelationIDs []string `json:"gap_pending_relation_ids,omitempty"`
	GapBoundaryIDs        []string `json:"gap_boundary_ids,omitempty"`
	PendingBoundaryIDs    []string `json:"pending_boundary_ids,omitempty"`
	BoundaryEventIDs      []string `json:"boundary_event_ids,omitempty"`
	DeferredRelations     []string `json:"deferred_relations,omitempty"`
}

type SyncState struct {
	NextBatch string                `json:"next_batch,omitempty"`
	Rooms     map[string]*RoomState `json:"rooms"`
}

func newSyncState() *SyncState {
	return &SyncState{Rooms: map[string]*RoomState{}}
}

func loadSyncState(blob string) (*SyncState, error) {
	state := newSyncState()
	if blob != "" {
		if err := json.Unmarshal([]byte(blob), state); err != nil {
			return nil, fmt.Errorf("decode Matrix sync state: %w", err)
		}
	}
	if state.Rooms == nil {
		state.Rooms = map[string]*RoomState{}
	}
	return state, nil
}

func (s *SyncState) marshal() (string, error) {
	b, err := json.Marshal(s, json.Deterministic(true))
	return string(b), err
}

type ImportOptions struct {
	UserID       string
	Full         bool
	Rooms        []string
	ExcludeRooms []string
	Progress     func(string)
}

type ImportSummary struct {
	RoomsProcessed      int64
	MessagesProcessed   int64
	MessagesAdded       int64
	Undecryptable       int64
	EventsSkipped       int64
	RelationsUnresolved int64
}

type Importer struct {
	store   *store.Store
	runtime *Runtime
	users   map[id.UserID]int64
}

func NewImporter(s *store.Store, runtime *Runtime) *Importer {
	return &Importer{store: s, runtime: runtime, users: map[id.UserID]int64{}}
}

func (imp *Importer) Import(ctx context.Context, opts ImportOptions) (sum *ImportSummary, err error) {
	if opts.UserID == "" {
		return nil, errors.New("matrix user ID required")
	}
	source, err := imp.store.GetSourceByTypeAndIdentifier(SourceType, opts.UserID)
	if err != nil {
		return nil, err
	}
	state := newSyncState()
	if !opts.Full {
		var encoded string
		checkpoint, checkpointErr := imp.store.GetLatestCheckpointedSyncByType(source.ID, SourceType)
		if checkpointErr == nil && checkpoint.CursorBefore.Valid {
			encoded = checkpoint.CursorBefore.String
		} else if checkpointErr != nil && !errors.Is(checkpointErr, store.ErrSyncRunNotFound) {
			return nil, checkpointErr
		} else {
			prev, prevErr := imp.store.GetLastSuccessfulSyncByType(source.ID, SourceType)
			if prevErr == nil && prev.CursorAfter.Valid {
				encoded = prev.CursorAfter.String
			} else if prevErr != nil && !errors.Is(prevErr, store.ErrSyncRunNotFound) {
				return nil, prevErr
			}
		}
		if encoded != "" {
			state, err = loadSyncState(encoded)
			if err != nil {
				return nil, err
			}
		}
	}
	syncID, err := imp.store.StartSync(source.ID, SourceType)
	if err != nil {
		return nil, err
	}
	scoped := imp.store.ScopedToSync(source.ID, syncID)
	imp = NewImporter(scoped, imp.runtime)
	sum = &ImportSummary{}
	completed := false
	defer func() {
		if err != nil && !completed {
			_ = scoped.FailSync(syncID, err.Error())
		}
	}()

	since := state.NextBatch
	// mautrix omits an empty set_presence parameter, which makes the homeserver
	// mark the client online. The client-server spec defines "offline" as "the
	// client is not marked as being online when it uses this API": it does not
	// set the account offline. Synapse only updates presence for values other
	// than offline, so this leaves presence set by other clients untouched;
	// "unavailable" would mark the account idle.
	resp, err := imp.runtime.Client.SyncRequest(ctx, 0, since, "", since == "", event.Presence("offline"))
	if err != nil {
		return sum, fmt.Errorf("matrix sync: %w", err)
	}
	directRooms, err := imp.directRooms(ctx)
	if err != nil {
		return sum, err
	}
	if err = imp.reclassifyArchivedRooms(source.ID, state, directRooms); err != nil {
		return sum, err
	}
	roomIDs := make([]id.RoomID, 0, len(resp.Rooms.Join))
	for roomID := range resp.Rooms.Join {
		if roomIncluded(roomID.String(), opts.Rooms, opts.ExcludeRooms) {
			roomIDs = append(roomIDs, roomID)
		}
	}
	slices.Sort(roomIDs)
	for _, roomID := range roomIDs {
		room := resp.Rooms.Join[roomID]
		if err = imp.importRoom(ctx, source.ID, syncID, roomID, room, directRooms[roomID], state, opts, sum); err != nil {
			return sum, err
		}
	}
	state.NextBatch = resp.NextBatch
	if err = scoped.RecomputeConversationStatsContext(ctx, source.ID); err != nil {
		return sum, fmt.Errorf("recompute Matrix conversation stats: %w", err)
	}
	finalState, err := state.marshal()
	if err != nil {
		return sum, err
	}
	if err = scoped.CompleteSyncAndUpdateSourceCursor(syncID, source.ID, finalState); err != nil {
		return sum, err
	}
	completed = true
	return sum, nil
}

func (imp *Importer) reclassifyArchivedRooms(sourceID int64, state *SyncState, directRooms map[id.RoomID]bool) error {
	for roomID := range state.Rooms {
		roomType := "group_chat"
		if directRooms[id.RoomID(roomID)] {
			roomType = "direct_chat"
		}
		if _, err := imp.store.EnsureConversationWithType(sourceID, roomID, roomType, ""); err != nil {
			return fmt.Errorf("reclassify Matrix room %s: %w", roomID, err)
		}
	}
	return nil
}

func roomIncluded(roomID string, include, exclude []string) bool {
	if slices.Contains(exclude, roomID) {
		return false
	}
	return len(include) == 0 || slices.Contains(include, roomID)
}

func (imp *Importer) directRooms(ctx context.Context) (map[id.RoomID]bool, error) {
	var direct event.DirectChatsEventContent
	if err := imp.runtime.Client.GetAccountData(ctx, event.AccountDataDirectChats.Type, &direct); err != nil {
		if errors.Is(err, mautrix.MNotFound) {
			return map[id.RoomID]bool{}, nil
		}
		return nil, fmt.Errorf("load Matrix direct-chat map: %w", err)
	}
	out := map[id.RoomID]bool{}
	for _, rooms := range direct {
		for _, roomID := range rooms {
			out[roomID] = true
		}
	}
	return out, nil
}

func (imp *Importer) importRoom(ctx context.Context, sourceID, syncID int64, roomID id.RoomID, room *mautrix.SyncJoinedRoom, direct bool, state *SyncState, opts ImportOptions, sum *ImportSummary) error {
	rs := state.Rooms[roomID.String()]
	hadRoomState := rs != nil
	wasBackfilled := rs != nil && rs.Backfilled
	if rs == nil || opts.Full {
		rs = &RoomState{
			PrevBatch:        room.Timeline.PrevBatch,
			BoundaryEventIDs: matrixEventIDs(room.Timeline.Events),
		}
		state.Rooms[roomID.String()] = rs
	}
	roomType := "group_chat"
	if direct {
		roomType = "direct_chat"
	}
	title, titlePresent := roomTitle(room.State.Events)
	if timelineTitle, timelineTitlePresent := roomTitle(room.Timeline.Events); timelineTitlePresent {
		title = timelineTitle
		titlePresent = true
	}
	members, memberNames, err := imp.members(ctx, roomID)
	if err != nil {
		return err
	}
	// Current membership makes a useful initial fallback title, but an
	// incremental /sync usually omits room state. Do not replace a previously
	// archived room name with the member list on every incremental run.
	if title == "" && !titlePresent && (!hadRoomState || opts.Full) {
		title = textutil.SanitizeTerminal(strings.Join(memberNames, ", "))
	}
	convID, err := imp.store.EnsureConversationWithType(sourceID, roomID.String(), roomType, title)
	if err != nil {
		return fmt.Errorf("ensure Matrix room %s: %w", roomID, err)
	}
	if titlePresent && title == "" {
		if err := imp.store.SetConversationTitle(sourceID, convID, ""); err != nil {
			return fmt.Errorf("clear Matrix room %s title: %w", roomID, err)
		}
	}
	if err := imp.store.ReplaceConversationParticipants(convID, members); err != nil {
		return fmt.Errorf("replace Matrix room members: %w", err)
	}
	if err := imp.store.SetConversationMemberCount(convID, len(members)); err != nil {
		return err
	}
	deferred, err := decodeDeferredRelations(rs.DeferredRelations)
	if err != nil {
		return fmt.Errorf("decode deferred Matrix relations for room %s: %w", roomID, err)
	}
	deferredIDs := make(map[id.EventID]struct{}, len(deferred))
	for _, evt := range deferred {
		deferredIDs[evt.ID] = struct{}{}
	}
	rememberDeferred := func(evt *event.Event) error {
		if evt == nil || evt.ID == "" {
			return nil
		}
		evt.RoomID = roomID
		if _, exists := deferredIDs[evt.ID]; exists {
			return nil
		}
		raw, marshalErr := json.Marshal(evt, json.Deterministic(true))
		if marshalErr != nil {
			return fmt.Errorf("encode deferred Matrix relation %s: %w", evt.ID, marshalErr)
		}
		deferred = append(deferred, evt)
		deferredIDs[evt.ID] = struct{}{}
		rs.DeferredRelations = append(rs.DeferredRelations, string(raw))
		return nil
	}
	forgetDeferred := func(eventID id.EventID) error {
		if _, exists := deferredIDs[eventID]; !exists {
			return nil
		}
		delete(deferredIDs, eventID)
		kept := deferred[:0]
		for _, candidate := range deferred {
			if candidate.ID != eventID {
				kept = append(kept, candidate)
			}
		}
		deferred = kept
		rs.DeferredRelations = rs.DeferredRelations[:0]
		for _, candidate := range deferred {
			raw, err := json.Marshal(candidate, json.Deterministic(true))
			if err != nil {
				return fmt.Errorf("encode retained Matrix relation %s: %w", candidate.ID, err)
			}
			rs.DeferredRelations = append(rs.DeferredRelations, string(raw))
		}
		return nil
	}
	persist := func(evt *event.Event) error {
		if evt != nil {
			evt.RoomID = roomID
		}
		err := imp.persistEvent(ctx, sourceID, convID, evt, sum)
		if errors.Is(err, errRelationTargetMissing) {
			return rememberDeferred(evt)
		}
		return err
	}
	ingest := func(evt *event.Event) error {
		if evt != nil && evt.Unsigned.RedactedBecause != nil {
			if err := forgetDeferred(evt.ID); err != nil {
				return err
			}
		}
		deferRelation, deferErr := shouldDeferRelation(evt)
		if deferErr != nil {
			return deferErr
		}
		if deferRelation {
			return rememberDeferred(evt)
		}
		return persist(evt)
	}
	for _, evt := range room.Timeline.Events {
		if err := ingest(evt); err != nil {
			return err
		}
	}
	// Save relations before the first history request. A failed page fetch
	// must not advance past work that needs an older target.
	if err := imp.checkpoint(syncID, state, sum); err != nil {
		return err
	}
	boundary := stringSet(rs.BoundaryEventIDs)
	boundaryVisible := containsMatrixEventID(room.Timeline.Events, boundary)
	bridgeInterruptedBackfill := hadRoomState && !wasBackfilled && !opts.Full && !boundaryVisible
	fillIncrementalGap := wasBackfilled && !opts.Full && room.Timeline.Limited && !boundaryVisible
	resumeGap := rs.GapBatch != ""
	legacyGapResume := resumeGap && (rs.LegacyGapBridge || len(rs.GapBoundaryIDs) == 0)
	if resumeGap || bridgeInterruptedBackfill || fillIncrementalGap {
		if legacyGapResume {
			rs.LegacyGapBridge = true
			if len(rs.GapBoundaryIDs) == 0 {
				rs.GapBoundaryIDs = slices.Clone(rs.BoundaryEventIDs)
			}
			if err := imp.checkpoint(syncID, state, sum); err != nil {
				return err
			}
		}
		if !resumeGap {
			rs.GapBatch = room.Timeline.PrevBatch
			rs.GapBoundaryIDs = slices.Clone(rs.BoundaryEventIDs)
			rs.PendingBoundaryIDs = matrixEventIDs(room.Timeline.Events)
			if err := imp.checkpoint(syncID, state, sum); err != nil {
				return err
			}
		}
		if err := imp.fillTimelineGap(ctx, syncID, roomID, rs, stringSet(rs.GapBoundaryIDs), state, sum, ingest); err != nil {
			return err
		}
		if resumeGap && len(rs.PendingBoundaryIDs) > 0 {
			completedBoundary := slices.Clone(rs.PendingBoundaryIDs)
			rs.BoundaryEventIDs = completedBoundary
			rs.GapBoundaryIDs = nil
			rs.PendingBoundaryIDs = nil
			if err := imp.checkpoint(syncID, state, sum); err != nil {
				return err
			}
			if !containsMatrixEventID(room.Timeline.Events, stringSet(completedBoundary)) {
				rs.GapBatch = room.Timeline.PrevBatch
				rs.GapBoundaryIDs = completedBoundary
				rs.PendingBoundaryIDs = matrixEventIDs(room.Timeline.Events)
				if err := imp.checkpoint(syncID, state, sum); err != nil {
					return err
				}
				if err := imp.fillTimelineGap(ctx, syncID, roomID, rs, stringSet(rs.GapBoundaryIDs), state, sum, ingest); err != nil {
					return err
				}
			}
		}
		if legacyGapResume && !containsMatrixEventID(room.Timeline.Events, boundary) {
			rs.LegacyGapBridge = false
			if err := imp.checkpoint(syncID, state, sum); err != nil {
				return err
			}
			rs.GapBatch = room.Timeline.PrevBatch
			rs.GapBoundaryIDs = slices.Clone(rs.BoundaryEventIDs)
			rs.PendingBoundaryIDs = matrixEventIDs(room.Timeline.Events)
			if err := imp.checkpoint(syncID, state, sum); err != nil {
				return err
			}
			if err := imp.fillTimelineGap(ctx, syncID, roomID, rs, boundary, state, sum, ingest); err != nil {
				return err
			}
		}
		rs.GapBoundaryIDs = nil
		rs.PendingBoundaryIDs = nil
		rs.LegacyGapBridge = false
	}
	if !rs.Backfilled {
		cursor := rs.PrevBatch
		for cursor != "" {
			previousCursor := cursor
			page, pageErr := imp.runtime.Client.Messages(ctx, roomID, cursor, "", mautrix.DirectionBackward, nil, pageSize)
			if pageErr != nil {
				return fmt.Errorf("backfill Matrix room %s: %w", roomID, pageErr)
			}
			for _, evt := range slices.Backward(page.Chunk) {
				if err := ingest(evt); err != nil {
					return err
				}
			}
			cursor = page.End
			rs.PrevBatch = cursor
			if err := imp.checkpoint(syncID, state, sum); err != nil {
				return err
			}
			if cursor == "" || cursor == previousCursor {
				break
			}
		}
	}
	// /messages walks from new to old, so a relation may arrive before its
	// target. Replay all relations after the history is present, oldest first,
	// so neither an original body nor an older edit can overwrite the latest.
	slices.SortFunc(deferred, func(a, b *event.Event) int {
		if priority := cmp.Compare(relationReplayPriority(a), relationReplayPriority(b)); priority != 0 {
			return priority
		}
		if a.Timestamp != b.Timestamp {
			return cmp.Compare(a.Timestamp, b.Timestamp)
		}
		return strings.Compare(a.ID.String(), b.ID.String())
	})
	remainingDeferred := make([]*event.Event, 0)
	for _, evt := range deferred {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := imp.replayDeferredRelation(ctx, sourceID, convID, evt, sum); err != nil {
			if errors.Is(err, errRelationTargetMissing) {
				sum.RelationsUnresolved++
				remainingDeferred = append(remainingDeferred, evt)
				continue
			}
			return err
		}
	}
	rs.Backfilled = true
	rs.GapBatch = ""
	rs.DeferredRelations = rs.DeferredRelations[:0]
	for _, evt := range remainingDeferred {
		raw, marshalErr := json.Marshal(evt, json.Deterministic(true))
		if marshalErr != nil {
			return fmt.Errorf("encode unresolved Matrix relation %s: %w", evt.ID, marshalErr)
		}
		rs.DeferredRelations = append(rs.DeferredRelations, string(raw))
	}
	if boundaryIDs := matrixEventIDs(room.Timeline.Events); len(boundaryIDs) > 0 {
		rs.BoundaryEventIDs = boundaryIDs
	}
	sum.RoomsProcessed++
	if opts.Progress != nil {
		opts.Progress(fmt.Sprintf("%s: %d members", roomID, len(members)))
	}
	return imp.checkpoint(syncID, state, sum)
}

func relationReplayPriority(evt *event.Event) int {
	if evt != nil && evt.Type == event.EventRedaction {
		return 1
	}
	return 0
}

func (imp *Importer) replayDeferredRelation(ctx context.Context, sourceID, convID int64, evt *event.Event, sum *ImportSummary) error {
	if evt != nil && (evt.Type == event.EventMessage || evt.Type == event.EventSticker) {
		if evt.Content.Parsed == nil {
			if err := evt.Content.ParseRaw(evt.Type); errors.Is(err, event.ErrUnsupportedContentType) {
				sum.EventsSkipped++
				return nil
			} else if err != nil {
				return fmt.Errorf("parse deferred Matrix event %s (%s): %w", evt.ID, evt.Type.Type, err)
			}
		}
		content := evt.Content.AsMessage()
		if content.RelatesTo.GetReplaceID() == "" && content.RelatesTo.GetReplyTo() != "" {
			return imp.resolveDeferredReply(ctx, sourceID, convID, evt, content.RelatesTo.GetReplyTo())
		}
	}
	return imp.persistEvent(ctx, sourceID, convID, evt, sum)
}

func (imp *Importer) resolveDeferredReply(ctx context.Context, sourceID, convID int64, evt *event.Event, target id.EventID) error {
	found, err := imp.store.MessageExistsBatch(sourceID, []string{evt.ID.String(), target.String()})
	if err != nil {
		return err
	}
	messageID, targetID := found[evt.ID.String()], found[target.String()]
	if messageID == 0 || targetID == 0 {
		return errRelationTargetMissing
	}
	targetMessage, err := imp.store.GetMessageRelationTarget(targetID)
	if err != nil {
		return err
	}
	if targetMessage.ConversationID != convID {
		return nil
	}
	return imp.store.SetMessageReplyContext(ctx, messageID, targetID)
}

func (imp *Importer) fillTimelineGap(
	ctx context.Context,
	syncID int64,
	roomID id.RoomID,
	rs *RoomState,
	boundary map[string]struct{},
	state *SyncState,
	sum *ImportSummary,
	persist func(*event.Event) error,
) error {
	cursor := rs.GapBatch
	relationBoundaries := make(map[string]struct{})
	if rs.GapBoundaryReached {
		for _, eventID := range rs.GapPendingRelationIDs {
			relationBoundaries[eventID] = struct{}{}
		}
	} else {
		deferred, err := decodeDeferredRelations(rs.DeferredRelations)
		if err != nil {
			return err
		}
		for _, evt := range deferred {
			if _, isBoundary := boundary[evt.ID.String()]; isBoundary {
				relationBoundaries[evt.ID.String()] = struct{}{}
			}
		}
	}
	reachedBoundary := rs.GapBoundaryReached
	for cursor != "" {
		previousCursor := cursor
		page, err := imp.runtime.Client.Messages(ctx, roomID, cursor, "", mautrix.DirectionBackward, nil, pageSize)
		if err != nil {
			return fmt.Errorf("fill Matrix timeline gap in room %s: %w", roomID, err)
		}
		beforeBoundary := len(page.Chunk)
		var strippedBoundaries []*event.Event
		for i, evt := range page.Chunk {
			if evt != nil {
				if _, isBoundary := boundary[evt.ID.String()]; isBoundary {
					if !reachedBoundary && beforeBoundary == len(page.Chunk) {
						beforeBoundary = i
					}
					if _, isRelationBoundary := relationBoundaries[evt.ID.String()]; isRelationBoundary && evt.Unsigned.RedactedBecause != nil {
						strippedBoundaries = append(strippedBoundaries, evt)
					}
					delete(relationBoundaries, evt.ID.String())
				}
			}
		}
		for _, strippedBoundary := range strippedBoundaries {
			if err := persist(strippedBoundary); err != nil {
				return err
			}
		}
		if !reachedBoundary {
			for i := beforeBoundary - 1; i >= 0; i-- {
				if err := persist(page.Chunk[i]); err != nil {
					return err
				}
			}
			reachedBoundary = beforeBoundary < len(page.Chunk)
		}
		cursor = page.End
		rs.GapBatch = cursor
		rs.GapBoundaryReached = reachedBoundary
		rs.GapPendingRelationIDs = sortedStringSet(relationBoundaries)
		if (reachedBoundary && len(relationBoundaries) == 0) || cursor == "" || cursor == previousCursor {
			rs.GapBatch = ""
			rs.GapBoundaryReached = false
			rs.GapPendingRelationIDs = nil
			return imp.checkpoint(syncID, state, sum)
		}
		if err := imp.checkpoint(syncID, state, sum); err != nil {
			return err
		}
	}
	return nil
}

func sortedStringSet(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	slices.Sort(result)
	return result
}

func shouldDeferRelation(evt *event.Event) (bool, error) {
	if evt == nil {
		return false, nil
	}
	switch evt.Type {
	case event.EventReaction, event.EventRedaction:
		return true, nil
	case event.EventMessage, event.EventSticker:
		if evt.Content.Parsed == nil {
			if err := evt.Content.ParseRaw(evt.Type); errors.Is(err, event.ErrUnsupportedContentType) {
				return false, nil
			} else if err != nil {
				return false, fmt.Errorf("parse Matrix event %s (%s): %w", evt.ID, evt.Type.Type, err)
			}
		}
		content := evt.Content.AsMessage()
		return content.RelatesTo != nil && content.RelatesTo.GetReplaceID() != "", nil
	default:
		return false, nil
	}
}

func decodeDeferredRelations(rawEvents []string) ([]*event.Event, error) {
	events := make([]*event.Event, 0, len(rawEvents))
	for _, raw := range rawEvents {
		var evt event.Event
		if err := json.Unmarshal([]byte(raw), &evt); err != nil {
			return nil, err
		}
		events = append(events, &evt)
	}
	return events, nil
}

func matrixEventIDs(events []*event.Event) []string {
	ids := make([]string, 0, len(events))
	for _, evt := range events {
		if evt != nil && evt.ID != "" {
			ids = append(ids, evt.ID.String())
		}
	}
	return ids
}

func stringSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		out[value] = struct{}{}
	}
	return out
}

func containsMatrixEventID(events []*event.Event, ids map[string]struct{}) bool {
	for _, evt := range events {
		if evt != nil {
			if _, ok := ids[evt.ID.String()]; ok {
				return true
			}
		}
	}
	return false
}

func (imp *Importer) checkpoint(syncID int64, state *SyncState, sum *ImportSummary) error {
	blob, err := state.marshal()
	if err != nil {
		return err
	}
	return imp.store.UpdateSyncCheckpoint(syncID, &store.Checkpoint{
		PageToken: blob, MessagesProcessed: sum.MessagesProcessed, MessagesAdded: sum.MessagesAdded,
	})
}

func roomTitle(events []*event.Event) (string, bool) {
	for _, evt := range slices.Backward(events) {
		if evt.Type == event.StateRoomName {
			if evt.Content.Parsed == nil {
				_ = evt.Content.ParseRaw(evt.Type)
			}
			// Room names are set by any member with permission and are shown in
			// single-line TUI rows, so strip terminal control sequences here.
			return strings.TrimSpace(textutil.SanitizeTerminal(evt.Content.AsRoomName().Name)), true
		}
	}
	return "", false
}

func (imp *Importer) members(ctx context.Context, roomID id.RoomID) ([]store.ConversationParticipantRef, []string, error) {
	resp, err := imp.runtime.Client.JoinedMembers(ctx, roomID)
	if err != nil {
		return nil, nil, fmt.Errorf("list Matrix room %s members: %w", roomID, err)
	}
	refs := make([]store.ConversationParticipantRef, 0, len(resp.Joined))
	names := make([]string, 0, len(resp.Joined))
	for userID, member := range resp.Joined {
		pid, err := imp.participant(userID, member.DisplayName)
		if err != nil {
			return nil, nil, err
		}
		refs = append(refs, store.ConversationParticipantRef{ParticipantID: pid, Role: "member"})
		if userID != imp.runtime.Client.UserID {
			name := strings.TrimSpace(member.DisplayName)
			if name == "" {
				name = userID.String()
			}
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return refs, names, nil
}

func (imp *Importer) participant(userID id.UserID, displayName string) (int64, error) {
	if pid := imp.users[userID]; pid != 0 {
		return pid, nil
	}
	pid, err := imp.store.EnsureParticipantByIdentifier(SourceType, userID.String(), displayName)
	if err != nil {
		return 0, fmt.Errorf("ensure Matrix participant %s: %w", userID, err)
	}
	imp.users[userID] = pid
	return pid, nil
}

func (imp *Importer) persistEvent(ctx context.Context, sourceID, convID int64, evt *event.Event, sum *ImportSummary) error {
	if evt == nil || evt.ID == "" {
		return nil
	}
	if evt.Unsigned.RedactedBecause != nil {
		if evt.Type == event.EventRedaction {
			redaction := *evt
			redaction.Unsigned.RedactedBecause = nil
			if redaction.Content.Parsed == nil {
				if err := redaction.Content.ParseRaw(redaction.Type); err != nil && !errors.Is(err, event.ErrUnsupportedContentType) {
					return fmt.Errorf("parse stripped Matrix redaction %s: %w", redaction.ID, err)
				}
			}
			target := redaction.Redacts
			if target == "" {
				target = redaction.Content.AsRedaction().Redacts
			}
			if target != "" {
				if err := imp.persistPlainEvent(ctx, sourceID, convID, &redaction, nil, sum); err != nil {
					return err
				}
			}
		}
		messageID, redactedEdit, redactErr := imp.store.RedactMatrixMessageVersion(sourceID, evt.ID.String())
		if redactErr != nil {
			return redactErr
		}
		if redactedEdit {
			if err := imp.ensureOriginalMessageVersion(sourceID, messageID); err != nil {
				return err
			}
			if err := imp.applyLatestMessageVersion(messageID); err != nil {
				return err
			}
			return imp.store.MarkMatrixEventRedacted(sourceID, evt.ID.String())
		}
		if evt.Type == event.EventReaction {
			if _, err := imp.store.DeleteReactionBySourceID(sourceID, evt.ID.String()); err != nil {
				return err
			}
			return imp.store.MarkMatrixEventRedacted(sourceID, evt.ID.String())
		}
		found, err := imp.store.MessageExistsBatch(sourceID, []string{evt.ID.String()})
		if err != nil {
			return err
		}
		if found[evt.ID.String()] != 0 {
			if err := imp.store.MarkMessageDeleted(sourceID, evt.ID.String()); err != nil {
				return err
			}
		}
		return imp.store.MarkMatrixEventRedacted(sourceID, evt.ID.String())
	}
	if evt.Content.Parsed == nil {
		if err := evt.Content.ParseRaw(evt.Type); errors.Is(err, event.ErrUnsupportedContentType) {
			sum.EventsSkipped++
			return nil
		} else if err != nil {
			return fmt.Errorf("parse Matrix event %s (%s): %w", evt.ID, evt.Type.Type, err)
		}
	}
	raw, err := json.Marshal(evt, json.Deterministic(true))
	if err != nil {
		return fmt.Errorf("encode Matrix event %s: %w", evt.ID, err)
	}
	if evt.Type == event.EventEncrypted {
		sum.Undecryptable++
		return imp.persistMessage(ctx, sourceID, convID, evt, raw, encryptedPlaceholder, nil, sum)
	}
	return imp.persistPlainEvent(ctx, sourceID, convID, evt, raw, sum)
}

func (imp *Importer) persistPlainEvent(ctx context.Context, sourceID, convID int64, evt *event.Event, raw []byte, sum *ImportSummary) error {
	switch evt.Type {
	case event.EventMessage, event.EventSticker:
		content := evt.Content.AsMessage()
		if target := content.RelatesTo.GetReplaceID(); target != "" && content.NewContent != nil {
			redacted, err := imp.store.IsMatrixEventRedacted(sourceID, evt.ID.String())
			if err != nil {
				return err
			}
			if redacted {
				return nil
			}
			return imp.persistEdit(sourceID, convID, evt, raw, target, content.NewContent)
		}
		return imp.persistMessage(ctx, sourceID, convID, evt, raw, messageBody(content), content, sum)
	case event.EventRedaction:
		target := evt.Redacts
		if target == "" {
			target = evt.Content.AsRedaction().Redacts
		}
		if target != "" {
			targetConversationID, owned, lookupErr := imp.store.MatrixEventConversation(sourceID, target.String())
			if lookupErr != nil {
				return lookupErr
			}
			if owned && targetConversationID != convID {
				return nil
			}
			redacted, redactErr := imp.store.IsMatrixEventRedacted(sourceID, target.String())
			if redactErr != nil {
				return redactErr
			}
			if redacted {
				return nil
			}
			messageID, redactedEdit, redactErr := imp.store.RedactMatrixMessageVersion(sourceID, target.String())
			if redactErr != nil {
				return redactErr
			}
			if redactedEdit {
				if err := imp.ensureOriginalMessageVersion(sourceID, messageID); err != nil {
					return err
				}
				if err := imp.applyLatestMessageVersion(messageID); err != nil {
					return err
				}
				return imp.store.MarkMatrixEventRedacted(sourceID, target.String())
			}
			deleted, deleteErr := imp.store.DeleteReactionBySourceID(sourceID, target.String())
			if deleteErr != nil {
				return deleteErr
			}
			if deleted {
				return imp.store.MarkMatrixEventRedacted(sourceID, target.String())
			}
			found, lookupErr := imp.store.MessageExistsBatch(sourceID, []string{target.String()})
			if lookupErr != nil {
				return lookupErr
			}
			if found[target.String()] != 0 {
				if err := imp.store.MarkMessageDeleted(sourceID, target.String()); err != nil {
					return err
				}
				return imp.store.MarkMatrixEventRedacted(sourceID, target.String())
			}
			return errRelationTargetMissing
		}
	case event.EventReaction:
		redacted, err := imp.store.IsMatrixEventRedacted(sourceID, evt.ID.String())
		if err != nil {
			return err
		}
		if redacted {
			return nil
		}
		return imp.persistReaction(sourceID, convID, evt)
	}
	return nil
}

func messageBody(content *event.MessageEventContent) string {
	if content == nil {
		return ""
	}
	if content.RelatesTo.GetReplyTo() != "" {
		withoutFallback := *content
		withoutFallback.RemoveReplyFallback()
		content = &withoutFallback
	}
	if content.MsgType.IsText() {
		if content.Format == event.FormatHTML && content.FormattedBody != "" {
			return textutil.SanitizeTerminalMultiline(msgmime.StripHTML(content.FormattedBody))
		}
		return textutil.SanitizeTerminalMultiline(content.Body)
	}
	switch content.MsgType {
	case event.MsgImage:
		return "[image]"
	case event.MsgVideo:
		return "[video]"
	case event.MsgAudio:
		return "[audio]"
	case event.MsgFile:
		return "[file: " + content.GetFileName() + "]"
	default:
		return textutil.SanitizeTerminalMultiline(content.Body)
	}
}

func (imp *Importer) persistMessage(ctx context.Context, sourceID, convID int64, evt *event.Event, raw []byte, body string, content *event.MessageEventContent, sum *ImportSummary) error {
	var replyToMessageID int64
	replyTargetMissing := false
	if content != nil && content.RelatesTo != nil {
		if reply := content.RelatesTo.GetReplyTo(); reply != "" {
			found, err := imp.store.MessageExistsBatch(sourceID, []string{reply.String()})
			if err != nil {
				return err
			}
			replyToMessageID = found[reply.String()]
			if replyToMessageID == 0 {
				replyTargetMissing = true
			} else {
				target, err := imp.store.GetMessageRelationTarget(replyToMessageID)
				if err != nil {
					return err
				}
				if target.ConversationID != convID {
					replyToMessageID = 0
				}
			}
		}
	}
	existing, err := imp.store.MessageExistsBatch(sourceID, []string{evt.ID.String()})
	if err != nil {
		return err
	}
	displayBody := body
	if existingID := existing[evt.ID.String()]; existingID != 0 {
		latest, latestErr := imp.store.LatestMatrixMessageVersion(existingID)
		if latestErr == nil {
			displayBody = latest.Body
		} else if !errors.Is(latestErr, sql.ErrNoRows) {
			return latestErr
		}
	}
	senderID, err := imp.participant(evt.Sender, "")
	if err != nil {
		return err
	}
	when := time.UnixMilli(evt.Timestamp).UTC()
	msg := &store.Message{
		ConversationID: convID, SourceID: sourceID, SourceMessageID: evt.ID.String(), MessageType: SourceType,
		SentAt: sql.NullTime{Time: when, Valid: evt.Timestamp > 0}, ReceivedAt: sql.NullTime{Time: when, Valid: evt.Timestamp > 0},
		SenderID: sql.NullInt64{Int64: senderID, Valid: senderID != 0}, IsFromMe: evt.Sender == imp.runtime.Client.UserID,
		Snippet: sql.NullString{String: snippet(displayBody), Valid: displayBody != ""}, SizeEstimate: int64(len(displayBody)),
	}
	messageID, err := imp.store.PersistMessageContext(ctx, &store.MessagePersistData{
		Message: msg, BodyText: sql.NullString{String: displayBody, Valid: displayBody != ""}, RawMIME: raw, RawFormat: rawFormat,
		FTS: &store.FTSDoc{Body: displayBody}, PreserveLabels: true,
	})
	if err != nil {
		return fmt.Errorf("persist Matrix event %s: %w", evt.ID, err)
	}
	if err := imp.store.UpsertMatrixMessageVersion(sourceID, store.MatrixMessageVersion{
		MessageID: messageID, EventID: evt.ID.String(), EventTS: evt.Timestamp,
		Body: body, RawEvent: raw, IsOriginal: true,
	}); err != nil {
		return err
	}
	if err := imp.applyLatestMessageVersion(messageID); err != nil {
		return err
	}
	if replyToMessageID != 0 {
		if err := imp.store.SetMessageReplyContext(ctx, messageID, replyToMessageID); err != nil {
			return err
		}
	}
	sum.MessagesProcessed++
	if existing[evt.ID.String()] == 0 {
		sum.MessagesAdded++
	}
	if replyTargetMissing {
		return errRelationTargetMissing
	}
	return nil
}

func (imp *Importer) persistEdit(sourceID, convID int64, evt *event.Event, raw []byte, target id.EventID, content *event.MessageEventContent) error {
	found, err := imp.store.MessageExistsBatch(sourceID, []string{target.String()})
	if err != nil {
		return err
	}
	messageID := found[target.String()]
	if messageID == 0 {
		return errRelationTargetMissing
	}
	targetMessage, err := imp.store.GetMessageRelationTarget(messageID)
	if err != nil {
		return err
	}
	editorID, err := imp.participant(evt.Sender, "")
	if err != nil {
		return err
	}
	if targetMessage.ConversationID != convID || !targetMessage.SenderID.Valid || targetMessage.SenderID.Int64 != editorID {
		return nil
	}
	if err := imp.ensureOriginalMessageVersion(sourceID, messageID); err != nil {
		return err
	}
	body := messageBody(content)
	if err := imp.store.UpsertMatrixMessageVersion(sourceID, store.MatrixMessageVersion{
		MessageID: messageID, EventID: evt.ID.String(), EventTS: evt.Timestamp, Body: body, RawEvent: raw,
	}); err != nil {
		return err
	}
	return imp.applyLatestMessageVersion(messageID)
}

func (imp *Importer) ensureOriginalMessageVersion(sourceID, messageID int64) error {
	hasOriginal, err := imp.store.HasOriginalMatrixMessageVersion(messageID)
	if err != nil || hasOriginal {
		return err
	}
	raw, err := imp.store.GetMessageRaw(messageID)
	if err != nil {
		return err
	}
	var original event.Event
	if err := json.Unmarshal(raw, &original); err != nil {
		return fmt.Errorf("decode retained Matrix event %d: %w", messageID, err)
	}
	if original.Content.Parsed == nil {
		if err := original.Content.ParseRaw(original.Type); err != nil {
			return fmt.Errorf("parse retained Matrix event %s: %w", original.ID, err)
		}
	}
	return imp.store.UpsertMatrixMessageVersion(sourceID, store.MatrixMessageVersion{
		MessageID: messageID, EventID: original.ID.String(), EventTS: original.Timestamp,
		Body: messageBody(original.Content.AsMessage()), RawEvent: raw, IsOriginal: true,
	})
}

func (imp *Importer) applyLatestMessageVersion(messageID int64) error {
	latest, err := imp.store.LatestMatrixMessageVersion(messageID)
	if err != nil {
		return err
	}
	body := latest.Body
	if err := imp.store.UpdateMessageDerivedTextAndSize(messageID,
		sql.NullString{String: body, Valid: body != ""}, sql.NullString{},
		sql.NullString{String: snippet(body), Valid: body != ""}, store.FTSDoc{Body: body}, int64(len(body))); err != nil {
		return err
	}
	return imp.store.SetMessageEditedState(messageID, !latest.IsOriginal)
}

func (imp *Importer) persistReaction(sourceID, convID int64, evt *event.Event) error {
	relation := evt.Content.AsReaction().RelatesTo
	found, err := imp.store.MessageExistsBatch(sourceID, []string{relation.EventID.String()})
	if err != nil {
		return err
	}
	messageID := found[relation.EventID.String()]
	if messageID == 0 {
		return errRelationTargetMissing
	}
	target, err := imp.store.GetMessageRelationTarget(messageID)
	if err != nil {
		return err
	}
	if target.ConversationID != convID {
		return nil
	}
	participantID, err := imp.participant(evt.Sender, "")
	if err != nil {
		return err
	}
	return imp.store.UpsertReactionWithSourceID(
		messageID, participantID, "emoji", relation.Key, evt.ID.String(), time.UnixMilli(evt.Timestamp).UTC(),
	)
}

func snippet(body string) string {
	runes := []rune(body)
	if len(runes) > 100 {
		return string(runes[:100])
	}
	return body
}
