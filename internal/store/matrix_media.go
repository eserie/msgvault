package store

import (
	"context"
	"strings"

	"go.kenn.io/msgvault/internal/attachmentpolicy"
)

// MediaConversation treats non-direct Matrix rooms as channels for media policy.
func MediaConversation(conversation attachmentpolicy.Conversation) attachmentpolicy.Conversation {
	if conversation.Type != "direct_chat" {
		conversation.Type = "channel"
	}
	return conversation
}

// ListMatrixLegacyMediaMessagesContext pages archived messages without Matrix attachment markers.
func (s *Store) ListMatrixLegacyMediaMessagesContext(ctx context.Context, sourceID, afterID int64, include, exclude []string, policy attachmentpolicy.Policy) ([]PendingAttachmentMessage, error) {
	var query strings.Builder
	query.WriteString(`SELECT m.id, m.source_message_id, c.source_conversation_id,
		c.conversation_type, COALESCE(c.participant_count, 0), COALESCE(CAST(c.metadata AS TEXT), '')
		FROM messages m JOIN conversations c ON c.id = m.conversation_id
		WHERE m.source_id = ? AND m.id > ?
		AND EXISTS (SELECT 1 FROM message_raw r WHERE r.message_id = m.id AND r.raw_format = 'matrix_json')
		AND NOT EXISTS (SELECT 1 FROM attachments a WHERE a.message_id = m.id AND a.source_attachment_id LIKE 'matrix:%')`)
	args := []any{sourceID, afterID}
	appendMatrixRoomFilters(&query, &args, include, exclude)
	query.WriteString(" ORDER BY m.id LIMIT 100")
	rows, err := s.db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var items []PendingAttachmentMessage
	for rows.Next() {
		var item PendingAttachmentMessage
		var metadata string
		if err := rows.Scan(&item.MessageID, &item.SourceMessageID, &item.ChatID, &item.ConversationType, &item.ParticipantCount, &metadata); err != nil {
			return nil, err
		}
		item.ParticipantCount = policyParticipantCount("matrix", item.ParticipantCount, metadata, policy)
		items = append(items, item)
	}
	return items, rows.Err()
}

// ListMatrixRetryableAttachmentMessagesContext includes media from source-redacted messages under current policy.
func (s *Store) ListMatrixRetryableAttachmentMessagesContext(ctx context.Context, sourceID, afterID int64, limit int, policy attachmentpolicy.Policy) ([]PendingAttachmentMessage, error) {
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	return s.listRetryableAttachmentMessagesPageContext(ctx, sourceID, "matrix:", policy, afterID, limit)
}

// MatrixOutstandingMedia counts durable pending and failed occurrences in the selected rooms.
func (s *Store) MatrixOutstandingMedia(ctx context.Context, sourceID int64, include, exclude []string) (pending, failed int64, err error) {
	var query strings.Builder
	query.WriteString(`SELECT COUNT(CASE WHEN a.attachment_state = 'pending' THEN 1 END),
		COUNT(CASE WHEN a.attachment_state = 'failed' THEN 1 END)
		FROM attachments a JOIN messages m ON m.id = a.message_id
		JOIN conversations c ON c.id = m.conversation_id
		WHERE m.source_id = ?
		AND a.source_attachment_id LIKE 'matrix:%'`)
	args := []any{sourceID}
	appendMatrixRoomFilters(&query, &args, include, exclude)
	err = s.db.QueryRowContext(ctx, query.String(), args...).Scan(&pending, &failed)
	return
}
