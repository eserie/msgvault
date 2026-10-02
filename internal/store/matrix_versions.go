package store

import (
	"context"
	"database/sql"
	"errors"
)

// HasOriginalMatrixMessageVersion reports whether the retained original has a
// durable version row, including when every edit has been redacted.
func (s *Store) HasOriginalMatrixMessageVersion(messageID int64) (bool, error) {
	var exists bool
	err := s.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM matrix_message_versions
		WHERE message_id = ? AND is_original = TRUE)`, messageID).Scan(&exists)
	return exists, err
}

// MatrixMessageVersion is one original Matrix message or replacement event.
type MatrixMessageVersion struct {
	MessageID  int64
	EventID    string
	EventTS    int64
	Body       string
	RawEvent   []byte
	IsOriginal bool
}

// MatrixEventConversation locates the room conversation that owns a retained
// Matrix message, edit, or reaction event.
func (s *Store) MatrixEventConversation(sourceID int64, eventID string) (int64, bool, error) {
	var conversationID int64
	err := s.db.QueryRow(`
		SELECT conversation_id FROM (
			SELECT m.conversation_id
			FROM matrix_message_versions v
			JOIN messages m ON m.id = v.message_id
			WHERE v.source_id = ? AND v.event_id = ?
			UNION ALL
			SELECT m.conversation_id
			FROM reaction_source_events rse
			JOIN reactions r ON r.id = rse.reaction_id
			JOIN messages m ON m.id = r.message_id
			WHERE rse.source_id = ? AND rse.source_reaction_id = ?
			UNION ALL
			SELECT m.conversation_id
			FROM messages m
			WHERE m.source_id = ? AND m.source_message_id = ?
		) owned_event
		LIMIT 1`, sourceID, eventID, sourceID, eventID, sourceID, eventID).Scan(&conversationID)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	return conversationID, err == nil, err
}

// MarkMatrixEventRedacted retains the fact that a Matrix event was redacted
// after its message, edit, or reaction payload has been removed.
func (s *Store) MarkMatrixEventRedacted(sourceID int64, eventID string) error {
	if err := s.requireSyncSource(sourceID); err != nil {
		return err
	}
	return s.withSyncSourceWriteContext(context.Background(), sourceID, func(q querier) error {
		_, err := q.Exec(s.dialect.InsertOrIgnore(`INSERT OR IGNORE INTO matrix_redacted_events
			(source_id, event_id) VALUES (?, ?)`), sourceID, eventID)
		return err
	})
}

// IsMatrixEventRedacted reports whether the event's redaction was already
// applied, even when applying it removed the target's lookup identity.
func (s *Store) IsMatrixEventRedacted(sourceID int64, eventID string) (bool, error) {
	var exists bool
	err := s.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM matrix_redacted_events
		WHERE source_id = ? AND event_id = ?)`, sourceID, eventID).Scan(&exists)
	return exists, err
}

// UpsertMatrixMessageVersion records a durable message version.
func (s *Store) UpsertMatrixMessageVersion(sourceID int64, version MatrixMessageVersion) error {
	if err := s.requireSyncSource(sourceID); err != nil {
		return err
	}
	return s.withSyncSourceWriteContext(context.Background(), sourceID, func(q querier) error {
		_, err := q.Exec(s.dialect.InsertOrIgnore(`INSERT OR IGNORE INTO matrix_message_versions
			(source_id, message_id, event_id, event_ts, body, raw_event, is_original, redacted)
			VALUES (?, ?, ?, ?, ?, ?, ?, FALSE)`), sourceID, version.MessageID, version.EventID,
			version.EventTS, version.Body, version.RawEvent, version.IsOriginal)
		return err
	})
}

// RedactMatrixMessageVersion marks an edit redacted. The original event is
// handled as a source deletion by the importer and is therefore not changed.
func (s *Store) RedactMatrixMessageVersion(sourceID int64, eventID string) (int64, bool, error) {
	if err := s.requireSyncSource(sourceID); err != nil {
		return 0, false, err
	}
	var messageID int64
	found := false
	err := s.withSyncSourceWriteContext(context.Background(), sourceID, func(q querier) error {
		err := q.QueryRow(`SELECT message_id FROM matrix_message_versions
			WHERE source_id = ? AND event_id = ? AND is_original = FALSE`, sourceID, eventID).Scan(&messageID)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		found = true
		_, err = q.Exec(`UPDATE matrix_message_versions SET redacted = TRUE
			WHERE source_id = ? AND event_id = ?`, sourceID, eventID)
		return err
	})
	return messageID, found, err
}

// LatestMatrixMessageVersion returns the newest surviving version by Matrix's
// stable timestamp/event-ID ordering.
func (s *Store) LatestMatrixMessageVersion(messageID int64) (MatrixMessageVersion, error) {
	var version MatrixMessageVersion
	err := s.db.QueryRow(`SELECT message_id, event_id, event_ts, COALESCE(body, ''), raw_event, is_original
		FROM matrix_message_versions
		WHERE message_id = ? AND redacted = FALSE
		ORDER BY is_original ASC, event_ts DESC, event_id DESC LIMIT 1`, messageID).Scan(
		&version.MessageID, &version.EventID, &version.EventTS, &version.Body, &version.RawEvent, &version.IsOriginal,
	)
	return version, err
}
