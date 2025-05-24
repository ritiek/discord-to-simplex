package main

import (
    "database/sql"
    "encoding/base64"
    "encoding/json"
    "flag"
    "fmt"
    "io/ioutil"
    "log"
    "os"
    "strings"
    "time"

    _ "github.com/xeodou/go-sqlcipher"
)

// Discord JSON export structure
type DiscordExport struct {
    Guild    DiscordGuild     `json:"guild"`
    Channel  DiscordChannel   `json:"channel"`
    Messages []DiscordMessage `json:"messages"`
}

type DiscordGuild struct {
    ID      string `json:"id"`
    Name    string `json:"name"`
    IconURL string `json:"iconUrl"`
}

type DiscordChannel struct {
    ID         string      `json:"id"`
    Type       string      `json:"type"`
    CategoryID interface{} `json:"categoryId"`
    Category   interface{} `json:"category"`
    Name       string      `json:"name"`
    Topic      interface{} `json:"topic"`
}

// Intermediary message format - universal structure
type UniversalMessage struct {
    // Core message data
    ID          string     `json:"id"`
    Content     string     `json:"content"`
    Timestamp   time.Time  `json:"timestamp"`
    EditedAt    *time.Time `json:"editedAt,omitempty"`
    
    // Author information
    Author      UniversalAuthor `json:"author"`
    
    // Message metadata
    MessageType string `json:"messageType"` // "text", "image", "file", "system", etc.
    Platform    string `json:"platform"`    // "discord", "telegram", "whatsapp", "slack", etc.
    
    // Rich content
    Attachments []UniversalAttachment `json:"attachments,omitempty"`
    Mentions    []UniversalMention    `json:"mentions,omitempty"`
    Reactions   []UniversalReaction   `json:"reactions,omitempty"`
    
    // Thread/reply information
    ReplyToID   *string `json:"replyToId,omitempty"`
    ThreadID    *string `json:"threadId,omitempty"`
    
    // Quote information for Discord replies
    QuotedMessage *QuotedMessage `json:"quotedMessage,omitempty"`
    
    // Platform-specific data (stored as JSON for flexibility)
    PlatformData map[string]interface{} `json:"platformData,omitempty"`
    
    // Message state
    IsDeleted   bool `json:"isDeleted"`
    IsPinned    bool `json:"isPinned"`
    IsSystem    bool `json:"isSystem"`
    IsSent      bool `json:"isSent"` // New field to track if message was sent by the user
}

type QuotedMessage struct {
    SharedMsgID []byte    `json:"sharedMsgId"`
    SentAt      time.Time `json:"sentAt"`
    Content     string    `json:"content"`
    IsSent      bool      `json:"isSent"`
}

type UniversalAuthor struct {
    ID          string  `json:"id"`
    Username    string  `json:"username"`
    DisplayName string  `json:"displayName"`
    AvatarURL   *string `json:"avatarUrl,omitempty"`
    IsBot       bool    `json:"isBot"`
    
    // Platform-specific author data
    PlatformData map[string]interface{} `json:"platformData,omitempty"`
}

type UniversalAttachment struct {
    ID       string `json:"id"`
    Filename string `json:"filename"`
    URL      string `json:"url"`
    MimeType string `json:"mimeType"`
    Size     int64  `json:"size"`
}

type UniversalMention struct {
    UserID   string `json:"userId"`
    Username string `json:"username"`
    Start    int    `json:"start"` // Position in text where mention starts
    Length   int    `json:"length"` // Length of the mention text
}

type UniversalReaction struct {
    Emoji   string   `json:"emoji"`
    Count   int      `json:"count"`
    UserIDs []string `json:"userIds"`
}

// Updated Discord message structures to match the JSON format
type DiscordMessage struct {
    ID                   string            `json:"id"`
    Type                 string            `json:"type"`
    Timestamp            string            `json:"timestamp"`
    TimestampEdited      *string           `json:"timestampEdited"`
    CallEndedTimestamp   *string           `json:"callEndedTimestamp"`
    IsPinned             bool              `json:"isPinned"`
    Content              string            `json:"content"`
    Author               DiscordAuthor     `json:"author"`
    Attachments          []interface{}     `json:"attachments"`
    Embeds               []interface{}     `json:"embeds"`
    Stickers             []interface{}     `json:"stickers"`
    Reactions            []interface{}     `json:"reactions"`
    Mentions             []DiscordMention  `json:"mentions"`
    Reference            *DiscordReference `json:"reference,omitempty"`
    InlineEmojis         []interface{}     `json:"inlineEmojis"`
}

type DiscordAuthor struct {
    ID           string      `json:"id"`
    Name         string      `json:"name"`
    Discriminator string     `json:"discriminator"`
    Nickname     string      `json:"nickname"`
    Color        interface{} `json:"color"`
    IsBot        bool        `json:"isBot"`
    Roles        []string    `json:"roles"`
    AvatarURL    string      `json:"avatarUrl"`
}

type DiscordMention struct {
    ID           string      `json:"id"`
    Name         string      `json:"name"`
    Discriminator string     `json:"discriminator"`
    Nickname     string      `json:"nickname"`
    Color        interface{} `json:"color"`
    IsBot        bool        `json:"isBot"`
    Roles        []string    `json:"roles"`
    AvatarURL    string      `json:"avatarUrl"`
}

type DiscordReference struct {
    MessageID string      `json:"messageId"`
    ChannelID string      `json:"channelId"`
    GuildID   interface{} `json:"guildId"`
}

// Prepared insert data structures
type MessageInsertData struct {
    MessageID   int
    ChatItemID  int
    SharedMsgID []byte
    Message     UniversalMessage
}

type BulkInsertData struct {
    Messages          []MessageInsertData
    StartMessageID    int
    StartChatItemID   int
    StartDeliveryRowID int
    // Add mapping from Discord message ID to shared_msg_id
    DiscordToSharedMsgID map[string][]byte
    // Add mapping from Discord message ID to full message data for quotes
    DiscordMessages map[string]DiscordMessage
}

// Platform-specific converters
func ConvertDiscordMessage(discordMsg DiscordMessage, myUsername string, discordToSharedMsgID map[string][]byte, discordMessages map[string]DiscordMessage) UniversalMessage {
    timestamp, _ := time.Parse(time.RFC3339, discordMsg.Timestamp)
    var editedAt *time.Time
    if discordMsg.TimestampEdited != nil {
        if parsed, err := time.Parse(time.RFC3339, *discordMsg.TimestampEdited); err == nil {
            editedAt = &parsed
        }
    }
    
    // Convert mentions
    var mentions []UniversalMention
    for _, mention := range discordMsg.Mentions {
        mentions = append(mentions, UniversalMention{
            UserID:   mention.ID,
            Username: mention.Name,
            Start:    0, // Discord doesn't provide position info in this format
            Length:   len(mention.Name),
        })
    }
    
    // Handle reply reference - use the mapping to get the correct shared_msg_id
    var replyToID *string
    var quotedMessage *QuotedMessage
    if discordMsg.Reference != nil {
        referencedDiscordID := discordMsg.Reference.MessageID
        if sharedMsgID, exists := discordToSharedMsgID[referencedDiscordID]; exists {
            // Convert shared_msg_id back to string for the universal format
            replyToIDStr := string(sharedMsgID)
            replyToID = &replyToIDStr
            
            // Get the quoted message data
            if quotedDiscordMsg, exists := discordMessages[referencedDiscordID]; exists {
                quotedTimestamp, _ := time.Parse(time.RFC3339, quotedDiscordMsg.Timestamp)
                quotedIsSent := quotedDiscordMsg.Author.Name == myUsername
                
                quotedMessage = &QuotedMessage{
                    SharedMsgID: sharedMsgID,
                    SentAt:      quotedTimestamp,
                    Content:     quotedDiscordMsg.Content,
                    IsSent:      quotedIsSent,
                }
            }
        } else {
            // If we can't find the referenced message, still store the original ID
            // This might happen if the referenced message is outside the export
            replyToID = &referencedDiscordID
        }
    }
    
    // Determine display name (prefer nickname, fallback to name)
    displayName := discordMsg.Author.Nickname
    if displayName == "" {
        displayName = discordMsg.Author.Name
    }
    
    // Check if this message was sent by the specified user
    isSent := discordMsg.Author.Name == myUsername
    
    return UniversalMessage{
        ID:            discordMsg.ID,
        Content:       discordMsg.Content,
        Timestamp:     timestamp,
        EditedAt:      editedAt,
        MessageType:   strings.ToLower(discordMsg.Type),
        Platform:      "discord",
        QuotedMessage: quotedMessage,
        Author: UniversalAuthor{
            ID:          discordMsg.Author.ID,
            Username:    discordMsg.Author.Name,
            DisplayName: displayName,
            AvatarURL:   &discordMsg.Author.AvatarURL,
            IsBot:       discordMsg.Author.IsBot,
            PlatformData: map[string]interface{}{
                "discriminator": discordMsg.Author.Discriminator,
                "roles":        discordMsg.Author.Roles,
                "color":        discordMsg.Author.Color,
            },
        },
        Mentions:  mentions,
        ReplyToID: replyToID,
        IsPinned:  discordMsg.IsPinned,
        IsSent:    isSent,
        PlatformData: map[string]interface{}{
            "embeds":       discordMsg.Embeds,
            "stickers":     discordMsg.Stickers,
            "inlineEmojis": discordMsg.InlineEmojis,
            "reference":    discordMsg.Reference,
        },
    }
}

// Interface for both *sql.DB and *sql.Tx
type Querier interface {
    QueryRow(query string, args ...interface{}) *sql.Row
    Query(query string, args ...interface{}) (*sql.Rows, error)
}

func getTableColumns(querier Querier, tableName string) ([]string, error) {
    rows, err := querier.Query(fmt.Sprintf("PRAGMA table_info(%s);", tableName))
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var columns []string
    for rows.Next() {
        var cid int
        var name, ctype string
        var notnull, pk int
        var dfltValue sql.NullString

        err = rows.Scan(&cid, &name, &ctype, &notnull, &dfltValue, &pk)
        if err != nil {
            return nil, err
        }
        columns = append(columns, name)
    }
    return columns, nil
}

func getTemplateRow(querier Querier, tableName string, idColumn string) (map[string]interface{}, error) {
    var templateID int
    query := fmt.Sprintf("SELECT MAX(%s) FROM %s", idColumn, tableName)
    err := querier.QueryRow(query).Scan(&templateID)
    if err != nil {
        return nil, fmt.Errorf("failed to get template %s: %w", idColumn, err)
    }

    columns, err := getTableColumns(querier, tableName)
    if err != nil {
        return nil, err
    }

    selectQuery := fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?", 
        strings.Join(columns, ", "), tableName, idColumn)
    
    row := querier.QueryRow(selectQuery, templateID)
    
    values := make([]interface{}, len(columns))
    valuePtrs := make([]interface{}, len(columns))
    for i := range values {
        valuePtrs[i] = &values[i]
    }
    
    err = row.Scan(valuePtrs...)
    if err != nil {
        return nil, err
    }
    
    result := make(map[string]interface{})
    for i, col := range columns {
        result[col] = values[i]
    }
    
    return result, nil
}

// Calculate safe chunk size based on number of columns and SQLite limit
func calculateChunkSize(numColumns int, maxParams int) int {
    if maxParams <= 0 {
        maxParams = 900 // Conservative limit below SQLite's 999
    }
    chunkSize := maxParams / numColumns
    if chunkSize < 1 {
        chunkSize = 1
    }
    return chunkSize
}

func bulkInsertMessages(tx *sql.Tx, data BulkInsertData) error {
    // Get template row
    templateRow, err := getTemplateRow(tx, "messages", "message_id")
    if err != nil {
        return fmt.Errorf("failed to get template row: %w", err)
    }

    columns, err := getTableColumns(tx, "messages")
    if err != nil {
        return err
    }

    // Calculate safe chunk size
    chunkSize := calculateChunkSize(len(columns), 900)
    
    // Process in chunks to avoid SQLite parameter limit
    for i := 0; i < len(data.Messages); i += chunkSize {
        end := i + chunkSize
        if end > len(data.Messages) {
            end = len(data.Messages)
        }
        
        chunk := data.Messages[i:end]
        
        // Build bulk insert query for this chunk
        placeholders := make([]string, len(chunk))
        args := make([]interface{}, 0, len(chunk)*len(columns))

        for j, msgData := range chunk {
            msg := msgData.Message
            
            // Create message body with proper structure
            encodedMsgID := base64.StdEncoding.EncodeToString([]byte(msg.ID))
            
            // Build params object with correct structure
            params := map[string]interface{}{
                "content": map[string]interface{}{
                    "text": msg.Content,
                    "type": "text", // Always use "text" for the content type
                },
            }

            // Add quote structure if this is a reply
            if msg.QuotedMessage != nil {
                params["quote"] = map[string]interface{}{
                    "content": map[string]interface{}{
                        "text": msg.QuotedMessage.Content,
                        "type": "text",
                    },
                    "msgRef": map[string]interface{}{
                        "msgId":  base64.StdEncoding.EncodeToString(msg.QuotedMessage.SharedMsgID),
                        // "sent":   msg.QuotedMessage.IsSent,
                        "sent":   false,
                        "sentAt": msg.QuotedMessage.SentAt.Format(time.RFC3339),
                    },
                }
            }

            msgBody := map[string]interface{}{
                "v":      "1-14",
                "msgId":  encodedMsgID,
                "event":  "x.msg.new",
                "params": params,
            }

            msgBodyBytes, err := json.Marshal(msgBody)
            if err != nil {
                return fmt.Errorf("failed to marshal message body: %w", err)
            }

            msgSent := 0
            if msg.IsSent {
                msgSent = 1
            }

            overrideFields := map[string]interface{}{
                "message_id":    msgData.MessageID,
                "shared_msg_id": msgData.SharedMsgID,
                "msg_body":      msgBodyBytes,
                "msg_sent":      msgSent,
                "created_at":    msg.Timestamp.Format("2006-01-02 15:04:05"),
                "updated_at":    msg.Timestamp.Format("2006-01-02 15:04:05"),
            }

						if msgSent == 1 {
							overrideFields["shared_msg_id_user"] = 1
						} else {
							overrideFields["shared_msg_id_user"] = nil
						}

            // Build row values
            rowValues := make([]interface{}, len(columns))
            for k, col := range columns {
                if val, override := overrideFields[col]; override {
                    rowValues[k] = val
                } else {
                    rowValues[k] = templateRow[col]
                }
            }

            placeholders[j] = "(" + strings.Repeat("?,", len(columns)-1) + "?)"
            args = append(args, rowValues...)
        }

        query := fmt.Sprintf("INSERT INTO messages (%s) VALUES %s",
            strings.Join(columns, ", "), strings.Join(placeholders, ", "))

        _, err = tx.Exec(query, args...)
        if err != nil {
            return fmt.Errorf("failed to execute chunk %d-%d: %w", i, end, err)
        }
    }
    
    return nil
}

func bulkInsertChatItems(tx *sql.Tx, data BulkInsertData) error {
    templateRow, err := getTemplateRow(tx, "chat_items", "chat_item_id")
    if err != nil {
        return fmt.Errorf("failed to get template row: %w", err)
    }

    columns, err := getTableColumns(tx, "chat_items")
    if err != nil {
        return err
    }

    // Calculate safe chunk size
    chunkSize := calculateChunkSize(len(columns), 900)
    
    // Process in chunks
    for i := 0; i < len(data.Messages); i += chunkSize {
        end := i + chunkSize
        if end > len(data.Messages) {
            end = len(data.Messages)
        }
        
        chunk := data.Messages[i:end]
        
        placeholders := make([]string, len(chunk))
        args := make([]interface{}, 0, len(chunk)*len(columns))

        for j, msgData := range chunk {
            msg := msgData.Message

            var itemContentTag string
            var itemStatus string
            if msg.IsSent {
                itemContentTag = "sndMsgContent"
                itemStatus = "snd_rcvd ok complete"
            } else {
                itemContentTag = "rcvMsgContent"
                itemStatus = "rcv_read"
            }

            itemContent := map[string]interface{}{
                itemContentTag: map[string]interface{}{
                    "msgContent": map[string]interface{}{
                        "type": "text",
                        "text": msg.Content,
                    },
                },
            }

            itemContentBytes, err := json.Marshal(itemContent)
            if err != nil {
                return fmt.Errorf("failed to marshal item_content: %w", err)
            }

            overrideFields := map[string]interface{}{
                "chat_item_id":      msgData.ChatItemID,
                "created_by_msg_id": msgData.MessageID,
                "shared_msg_id":     msgData.SharedMsgID,
                "item_content":      string(itemContentBytes),
                "item_text":         msg.Content,
                "item_content_tag":  itemContentTag,
                "item_status":       itemStatus,
                "item_ts":           msg.Timestamp.Format("2006-01-02 15:04:05"),
                "created_at":        msg.Timestamp.Format("2006-01-02 15:04:05"),
                "updated_at":        msg.Timestamp.Format("2006-01-02 15:04:05"),
            }

            // Handle quoted message fields for Discord replies
            if msg.QuotedMessage != nil {
                quotedContent := map[string]interface{}{
                    "type": "text",
                    "text": msg.QuotedMessage.Content,
                }
                quotedContentBytes, err := json.Marshal(quotedContent)
                if err != nil {
                    return fmt.Errorf("failed to marshal quoted_content: %w", err)
                }

                quotedSent := 0
                if msg.QuotedMessage.IsSent {
                    quotedSent = 1
                }

                overrideFields["quoted_shared_msg_id"] = string(msg.QuotedMessage.SharedMsgID)
                overrideFields["quoted_sent_at"] = msg.QuotedMessage.SentAt.Format("2006-01-02 15:04:05")
                overrideFields["quoted_content"] = string(quotedContentBytes)
                overrideFields["quoted_sent"] = quotedSent
            } else {
                overrideFields["quoted_shared_msg_id"] = nil
                overrideFields["quoted_sent_at"] = nil
                overrideFields["quoted_content"] = nil
                overrideFields["quoted_sent"] = nil
						}

						// FIXME: Need to quoted replys working.
						overrideFields["quoted_shared_msg_id"] = nil
						overrideFields["quoted_sent_at"] = nil
						overrideFields["quoted_content"] = nil
						overrideFields["quoted_sent"] = nil

            rowValues := make([]interface{}, len(columns))
            for k, col := range columns {
                if val, override := overrideFields[col]; override {
                    rowValues[k] = val
                } else {
                    rowValues[k] = templateRow[col]
                }
            }

            placeholders[j] = "(" + strings.Repeat("?,", len(columns)-1) + "?)"
            args = append(args, rowValues...)
        }

        query := fmt.Sprintf("INSERT INTO chat_items (%s) VALUES %s",
            strings.Join(columns, ", "), strings.Join(placeholders, ", "))

        _, err = tx.Exec(query, args...)
        if err != nil {
            return fmt.Errorf("failed to execute chunk %d-%d: %w", i, end, err)
        }
    }
    
    return nil
}

func bulkInsertChatItemMessages(tx *sql.Tx, data BulkInsertData) error {
    templateRow, err := getTemplateRow(tx, "chat_item_messages", "rowid")
    if err != nil {
        return fmt.Errorf("failed to get template row: %w", err)
    }
    columns, err := getTableColumns(tx, "chat_item_messages")
    if err != nil {
        return err
    }
    
    // Get next available rowid
    var nextRowID int
    err = tx.QueryRow("SELECT COALESCE(MAX(rowid), 0) + 1 FROM chat_item_messages").Scan(&nextRowID)
    if err != nil {
        return fmt.Errorf("failed to get next rowid: %w", err)
    }
    
    // Calculate safe chunk size
    chunkSize := calculateChunkSize(len(columns), 900)
    
    // Process in chunks
    for i := 0; i < len(data.Messages); i += chunkSize {
        end := i + chunkSize
        if end > len(data.Messages) {
            end = len(data.Messages)
        }
        
        chunk := data.Messages[i:end]
        
        placeholders := make([]string, len(chunk))
        args := make([]interface{}, 0, len(chunk)*len(columns))
        for j, msgData := range chunk {
            msg := msgData.Message
            overrideFields := map[string]interface{}{
                "rowid":        nextRowID + i + j,
                "chat_item_id": msgData.ChatItemID,
                "message_id":   msgData.MessageID,
                "created_at":   msg.Timestamp.Format("2006-01-02 15:04:05"),
                "updated_at":   msg.Timestamp.Format("2006-01-02 15:04:05"),
            }
            rowValues := make([]interface{}, len(columns))
            for k, col := range columns {
                if val, override := overrideFields[col]; override {
                    rowValues[k] = val
                } else {
                    rowValues[k] = templateRow[col]
                }
            }
            placeholders[j] = "(" + strings.Repeat("?,", len(columns)-1) + "?)"
            args = append(args, rowValues...)
        }
        query := fmt.Sprintf("INSERT INTO chat_item_messages (%s) VALUES %s",
            strings.Join(columns, ", "), strings.Join(placeholders, ", "))
        _, err = tx.Exec(query, args...)
        if err != nil {
            return fmt.Errorf("failed to execute chunk %d-%d: %w", i, end, err)
        }
    }
    
    return nil
}

func bulkInsertMsgDeliveries(tx *sql.Tx, data BulkInsertData) error {
    templateRow, err := getTemplateRow(tx, "msg_deliveries", "msg_delivery_id")
    if err != nil {
        return fmt.Errorf("failed to get template row: %w", err)
    }

    columns, err := getTableColumns(tx, "msg_deliveries")
    if err != nil {
        return err
    }

    // Get max agent_msg_id
    var maxAgentMsgID int
    err = tx.QueryRow("SELECT COALESCE(MAX(agent_msg_id), 0) FROM msg_deliveries").Scan(&maxAgentMsgID)
    if err != nil {
        return fmt.Errorf("failed to get max agent_msg_id: %w", err)
    }

    // Calculate safe chunk size
    chunkSize := calculateChunkSize(len(columns), 900)
    
    // Process in chunks
    for i := 0; i < len(data.Messages); i += chunkSize {
        end := i + chunkSize
        if end > len(data.Messages) {
            end = len(data.Messages)
        }
        
        chunk := data.Messages[i:end]
        
        placeholders := make([]string, len(chunk))
        args := make([]interface{}, 0, len(chunk)*len(columns))

        for j, msgData := range chunk {
            msg := msgData.Message

            var itemStatus string
            if msg.IsSent {
                itemStatus = "snd_rcvd ok"
            } else {
                itemStatus = "rcv_read"
            }

            overrideFields := map[string]interface{}{
                "msg_delivery_id": msgData.MessageID,
                "message_id":      msgData.MessageID,
                "agent_msg_id":    maxAgentMsgID + 1 + i + j,
                "agent_msg_meta":  nil,
                "delivery_status": itemStatus,
                "chat_ts":         msg.Timestamp.Format("2006-01-02 15:04:05"),
                "created_at":      msg.Timestamp.Format("2006-01-02 15:04:05"),
                "updated_at":      msg.Timestamp.Format("2006-01-02 15:04:05"),
            }

            rowValues := make([]interface{}, len(columns))
            for k, col := range columns {
                if val, override := overrideFields[col]; override {
                    rowValues[k] = val
                } else {
                    rowValues[k] = templateRow[col]
                }
            }

            placeholders[j] = "(" + strings.Repeat("?,", len(columns)-1) + "?)"
            args = append(args, rowValues...)
        }

        query := fmt.Sprintf("INSERT INTO msg_deliveries (%s) VALUES %s",
            strings.Join(columns, ", "), strings.Join(placeholders, ", "))

        _, err = tx.Exec(query, args...)
        if err != nil {
            return fmt.Errorf("failed to execute chunk %d-%d: %w", i, end, err)
        }
    }
    
    return nil
}

func bulkInsertUniversalMessages(db *sql.DB, messages []UniversalMessage, startMessageID int) error {
    // Start transaction
    tx, err := db.Begin()
    if err != nil {
        return fmt.Errorf("failed to begin transaction: %w", err)
    }
    defer tx.Rollback()

    // Get starting IDs
    var maxChatItemID int
    err = tx.QueryRow("SELECT COALESCE(MAX(chat_item_id), 0) FROM chat_items").Scan(&maxChatItemID)
    if err != nil {
        return fmt.Errorf("failed to get max chat_item_id: %w", err)
    }

    // Prepare bulk insert data
    bulkData := BulkInsertData{
        Messages:             make([]MessageInsertData, len(messages)),
        StartMessageID:       startMessageID,
        StartChatItemID:      maxChatItemID + 1,
        DiscordToSharedMsgID: make(map[string][]byte),
    }

    for i, msg := range messages {
        messageID := startMessageID + i
        chatItemID := maxChatItemID + 1 + i
        sharedMsgID := []byte(msg.ID)

        bulkData.Messages[i] = MessageInsertData{
            MessageID:   messageID,
            ChatItemID:  chatItemID,
            SharedMsgID: sharedMsgID,
            Message:     msg,
        }

        // Build the mapping from Discord message ID to the shared_msg_id that will be stored
        bulkData.DiscordToSharedMsgID[msg.ID] = sharedMsgID
    }

    // Perform bulk inserts
    fmt.Printf("Inserting %d messages...\n", len(messages))
    
    err = bulkInsertMessages(tx, bulkData)
    if err != nil {
        return fmt.Errorf("failed to bulk insert messages: %w", err)
    }

    err = bulkInsertChatItems(tx, bulkData)
    if err != nil {
        return fmt.Errorf("failed to bulk insert chat items: %w", err)
    }

    err = bulkInsertChatItemMessages(tx, bulkData)
    if err != nil {
        return fmt.Errorf("failed to bulk insert chat item messages: %w", err)
    }

    err = bulkInsertMsgDeliveries(tx, bulkData)
    if err != nil {
        return fmt.Errorf("failed to bulk insert msg deliveries: %w", err)
    }

    // Commit transaction
    err = tx.Commit()
    if err != nil {
        return fmt.Errorf("failed to commit transaction: %w", err)
    }

    return nil
}

func loadDiscordExport(filePath string) (*DiscordExport, error) {
    data, err := ioutil.ReadFile(filePath)
    if err != nil {
        return nil, fmt.Errorf("failed to read file: %w", err)
    }

    var export DiscordExport
    err = json.Unmarshal(data, &export)
    if err != nil {
        return nil, fmt.Errorf("failed to parse JSON: %w", err)
    }

    return &export, nil
}

func main() {
    // Command line arguments
    var jsonFilePath string
    var myUsername string
    var dbPath string
    var batchSize int

    flag.StringVar(&jsonFilePath, "json", "", "Path to Discord JSON export file (required)")
    flag.StringVar(&myUsername, "me", "", "Your Discord username to identify sent messages (required)")
    flag.StringVar(&dbPath, "db", "/home/ritiek/.local/share/simplex/simplex_v1_chat.db", "Path to SQLCipher database")
    flag.IntVar(&batchSize, "batch", 5000, "Batch size for bulk inserts (default: 5000)")
    flag.Parse()

    if jsonFilePath == "" {
        log.Fatal("JSON file path is required. Use -json flag.")
    }
    if myUsername == "" {
        log.Fatal("Username is required. Use -me flag.")
    }

    password := os.Getenv("SQLCIPHER_KEY")
    if password == "" {
        log.Fatal("SQLCIPHER_KEY environment variable not set")
    }

    // Load Discord export
    fmt.Printf("Loading Discord export from: %s\n", jsonFilePath)
    export, err := loadDiscordExport(jsonFilePath)
    if err != nil {
        log.Fatalf("Failed to load Discord export: %v", err)
    }

    fmt.Printf("Loaded export for channel: %s (%d messages)\n", export.Channel.Name, len(export.Messages))
    fmt.Printf("Your username: %s\n", myUsername)
    fmt.Printf("Batch size: %d\n\n", batchSize)
    
    // Connect to database
    dsn := fmt.Sprintf("%s?_key=%s&_busy_timeout=30000", dbPath, password)
    db, err := sql.Open("sqlite3", dsn)
    if err != nil {
        log.Fatalf("Failed to open database: %v", err)
    }
    defer db.Close()

    // Test connection
    err = db.Ping()
    if err != nil {
        log.Fatalf("Failed to connect to database: %v", err)
    }

    // Get starting message ID
    var startMessageID int
    err = db.QueryRow("SELECT COALESCE(MAX(message_id), 0) + 1 FROM messages").Scan(&startMessageID)
    if err != nil {
        log.Fatalf("Failed to get starting message ID: %v", err)
    }

    fmt.Printf("Starting message ID: %d\n", startMessageID)

    // First pass: Build Discord ID to shared_msg_id mapping for the entire dataset
    fmt.Println("Building message ID mapping...")
    discordToSharedMsgID := make(map[string][]byte)
    discordMessages := make(map[string]DiscordMessage)
    for i, discordMsg := range export.Messages {
        sharedMsgID := []byte(discordMsg.ID)
        discordToSharedMsgID[discordMsg.ID] = sharedMsgID
        discordMessages[discordMsg.ID] = discordMsg
        
        // For debugging: print first few mappings
        if i < 5 {
            fmt.Printf("Mapping Discord ID %s to shared_msg_id %s\n", discordMsg.ID, string(sharedMsgID))
        }
    }

    // Second pass: Convert all messages to universal format with proper reply mapping
    fmt.Println("Converting Discord messages to universal format...")
    universalMessages := make([]UniversalMessage, 0, len(export.Messages))

    for _, discordMsg := range export.Messages {
        universalMsg := ConvertDiscordMessage(discordMsg, myUsername, discordToSharedMsgID, discordMessages)
        universalMessages = append(universalMessages, universalMsg)
    }

    // Process messages in batches
    // totalMessages := 40
    totalMessages := len(universalMessages)
    fmt.Printf("Processing %d messages in batches of %d...\n", totalMessages, batchSize)

    for i := 0; i < totalMessages; i += batchSize {
        end := i + batchSize
        if end > totalMessages {
            end = totalMessages
        }

        batch := universalMessages[i:end]
        batchStartID := startMessageID + i

        fmt.Printf("Processing batch %d-%d...\n", i+1, end)

        err = bulkInsertUniversalMessages(db, batch, batchStartID)
        if err != nil {
            log.Fatalf("Failed to insert batch %d-%d: %v", i+1, end, err)
        }

        fmt.Printf("Successfully inserted batch %d-%d\n", i+1, end)
    }
}
