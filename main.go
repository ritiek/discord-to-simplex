package main

import (
    "database/sql"
    "encoding/base64"
    "encoding/json"
    "path/filepath"
    "flag"
    "fmt"
    "io"
    "io/ioutil"
    "log"
    "os"
    "os/exec"
    "strconv"
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

type DiscordAttachment struct {
    ID            string `json:"id"`
    URL           string `json:"url"`
    FileName      string `json:"fileName"`
    FileSizeBytes int64  `json:"fileSizeBytes"`
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

// Helper function to read and encode image as base64
func encodeImageToBase64(imagePath string) (string, error) {
    imageData, err := ioutil.ReadFile(imagePath)
    if err != nil {
        return "", fmt.Errorf("failed to read image file %s: %w", imagePath, err)
    }

    // Determine MIME type based on file extension
    ext := strings.ToLower(filepath.Ext(imagePath))
    var mimeType string
    switch ext {
		case ".jpg":
        mimeType = "image/jpg"
			case ".jpeg":
        mimeType = "image/jpeg"
    case ".png":
        mimeType = "image/png"
    case ".gif":
        mimeType = "image/gif"
    default:
        mimeType = "image/jpg" // default fallback
    }

    return fmt.Sprintf("data:%s;base64,%s", mimeType, base64.StdEncoding.EncodeToString(imageData)), nil
}

// Function to generate video thumbnail using ffmpeg and get video duration
func generateVideoThumbnail(videoPath string) (string, int, error) {
    // Create temporary directory for thumbnail
    tempDir := "/tmp/video_thumbnails"
    if err := os.MkdirAll(tempDir, 0755); err != nil {
        return "", 0, fmt.Errorf("failed to create temp directory: %w", err)
    }
    
    // Generate unique thumbnail filename
    thumbnailPath := filepath.Join(tempDir, fmt.Sprintf("thumb_%d.jpg", os.Getpid()))
    
    // Get video duration first
    durationCmd := exec.Command("ffprobe", "-v", "quiet", "-show_entries", "format=duration", "-of", "csv=p=0", videoPath)
    durationOutput, err := durationCmd.Output()
    if err != nil {
        return "", 0, fmt.Errorf("failed to get video duration: %w", err)
    }
    
    // Parse duration (convert from float seconds to int)
    var duration int
    if len(durationOutput) > 0 {
        durationStr := strings.TrimSpace(string(durationOutput))
        if durationFloat := parseFloat(durationStr); durationFloat > 0 {
            duration = int(durationFloat)
        } else {
            duration = 86 // Default fallback duration
        }
    } else {
        duration = 86 // Default fallback duration
    }
    
    // Use ffmpeg to extract thumbnail at 1 second mark
    cmd := exec.Command("ffmpeg", "-i", videoPath, "-ss", "00:00:01", "-vframes", "1", "-f", "image2", "-s", "320x240", thumbnailPath, "-y")
    cmd.Stderr = nil // Suppress ffmpeg output
    
    if err := cmd.Run(); err != nil {
        // If ffmpeg fails, try without seeking
        cmd = exec.Command("ffmpeg", "-i", videoPath, "-vframes", "1", "-f", "image2", "-s", "320x240", thumbnailPath, "-y")
        cmd.Stderr = nil
        if err := cmd.Run(); err != nil {
            return "", 0, fmt.Errorf("failed to generate thumbnail with ffmpeg: %w", err)
        }
    }
    
    // Read the thumbnail file
    thumbnailData, err := ioutil.ReadFile(thumbnailPath)
    if err != nil {
        return "", 0, fmt.Errorf("failed to read thumbnail: %w", err)
    }
    
    // Clean up temp file
    os.Remove(thumbnailPath)
    
    // Return base64 encoded thumbnail and duration
    return fmt.Sprintf("data:image/jpg;base64,%s", base64.StdEncoding.EncodeToString(thumbnailData)), duration, nil
}

// Helper function to parse float from string
func parseFloat(s string) float64 {
    if f, err := strconv.ParseFloat(s, 64); err == nil {
        return f
    }
    return 0
}

// Helper function to copy video file to SimpleX files directory
func copyFileToSimplexDir(sourcePath, filename string) error {
    simplexFilesDir := "/home/ritiek/.local/share/simplex/simplex_v1_files"
    
    // Ensure SimpleX files directory exists
    if err := os.MkdirAll(simplexFilesDir, 0755); err != nil {
        return fmt.Errorf("failed to create SimpleX files directory: %w", err)
    }
    
    // Copy file
    sourceFile, err := os.Open(sourcePath)
    if err != nil {
        return fmt.Errorf("failed to open source file: %w", err)
    }
    defer sourceFile.Close()
    
    destPath := filepath.Join(simplexFilesDir, filename)
    destFile, err := os.Create(destPath)
    if err != nil {
        return fmt.Errorf("failed to create destination file: %w", err)
    }
    defer destFile.Close()
    
    _, err = io.Copy(destFile, sourceFile)
    if err != nil {
        return fmt.Errorf("failed to copy file: %w", err)
    }
    
    return nil
}

// Deprecated: use copyFileToSimplexDir instead
func copyVideoToSimplexDir(sourcePath, filename string) error {
    return copyFileToSimplexDir(sourcePath, filename)
}

func getContactIDByName(db *sql.DB, contactName string) (int, error) {
    var contactID int
    query := `SELECT c.contact_id FROM contacts c 
              LEFT JOIN contact_profiles cp ON c.contact_profile_id = cp.contact_profile_id 
              WHERE c.deleted = 0 AND (c.local_display_name = ? OR cp.display_name = ?) 
              LIMIT 1`
    err := db.QueryRow(query, contactName, contactName).Scan(&contactID)
    if err != nil {
        if err == sql.ErrNoRows {
            return 0, fmt.Errorf("contact '%s' not found", contactName)
        }
        return 0, fmt.Errorf("failed to lookup contact: %w", err)
    }
    return contactID, nil
}

// Platform-specific converters
func ConvertDiscordMessage(discordMsg DiscordMessage, myUsername string, discordToSharedMsgID map[string][]byte, discordMessages map[string]DiscordMessage, jsonDir string) UniversalMessage {
    timestamp, _ := time.Parse(time.RFC3339, discordMsg.Timestamp)
    var editedAt *time.Time
    if discordMsg.TimestampEdited != nil {
        if parsed, err := time.Parse(time.RFC3339, *discordMsg.TimestampEdited); err == nil {
            editedAt = &parsed
        }
    }

    // Handle attachments
    var attachments []UniversalAttachment
    var messageType string = "text"
    if len(discordMsg.Attachments) > 0 {
        for _, att := range discordMsg.Attachments {
            if attMap, ok := att.(map[string]interface{}); ok {
                filename := fmt.Sprintf("%v", attMap["fileName"])
                
                // Determine message type based on file extension
                ext := strings.ToLower(filepath.Ext(filename))
                switch ext {
                case ".jpg", ".jpeg", ".png", ".gif", ".webp":
                    messageType = "image"
                case ".mp4", ".webm", ".mov", ".avi":
                    messageType = "video"
                case ".mp3", ".wav", ".m4a", ".ogg":
                    messageType = "voice"
                default:
                    messageType = "file"
                }
                
                attachments = append(attachments, UniversalAttachment{
                    ID:       fmt.Sprintf("%v", attMap["id"]),
                    Filename: filename,
                    URL:      fmt.Sprintf("%v", attMap["url"]),
                    Size:     int64(attMap["fileSizeBytes"].(float64)),
                })
            }
        }
        
        // If no message type was set (no attachments processed), default to file
        if messageType == "text" && len(attachments) > 0 {
            messageType = "file"
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
        MessageType:   messageType,
        Attachments:   attachments,
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
    var templateID sql.NullInt64
    query := fmt.Sprintf("SELECT MAX(%s) FROM %s", idColumn, tableName)
    err := querier.QueryRow(query).Scan(&templateID)
    if err != nil {
        return nil, fmt.Errorf("failed to get template %s: %w", idColumn, err)
    }

    // If database is empty (MAX returns NULL), return empty map
    if !templateID.Valid {
        return make(map[string]interface{}), nil
    }

    columns, err := getTableColumns(querier, tableName)
    if err != nil {
        return nil, err
    }

    selectQuery := fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?",
        strings.Join(columns, ", "), tableName, idColumn)

    row := querier.QueryRow(selectQuery, templateID.Int64)

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

func bulkInsertMessages(tx *sql.Tx, data BulkInsertData, jsonDir string, contactID int) error {
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

            var content map[string]interface{}
            var fileInfo map[string]interface{}

            // Handle different message types with attachments
            if len(msg.Attachments) > 0 {
                attachment := msg.Attachments[0] // Use first attachment
                
                switch msg.MessageType {
                case "image":
                    imagePath := filepath.Join(jsonDir, attachment.URL)
                    imageBase64, err := encodeImageToBase64(imagePath)
                    if err != nil {
                        log.Printf("Warning: failed to encode image %s: %v", imagePath, err)
                        // Fallback to text with file info
                        content = map[string]interface{}{
                            "text": fmt.Sprintf("[Image: %s]%s", attachment.Filename, 
                                func() string { if msg.Content != "" { return "\n" + msg.Content }; return "" }()),
                            "type": "text",
                        }
                    } else {
                        content = map[string]interface{}{
                            "image": imageBase64,
                            "text":  msg.Content,
                            "type":  "image",
                        }
                        fileInfo = map[string]interface{}{
                            "fileDescr": map[string]interface{}{
                                "fileDescrComplete": false,
                                "fileDescrPartNo":   0,
                                "fileDescrText":     "",
                            },
                            "fileName": attachment.Filename,
                            "fileSize": attachment.Size,
                        }
                    }
                    
                case "video":
                    // For videos, try to generate thumbnail and get duration
                    videoPath := filepath.Join(jsonDir, attachment.URL)
                    thumbnailBase64, duration, err := generateVideoThumbnail(videoPath)
                    if err != nil {
                        log.Printf("Warning: failed to generate video thumbnail for %s: %v", attachment.Filename, err)
                        // Fallback to file type without thumbnail
                        content = map[string]interface{}{
                            "type": "file",
                            "text": msg.Content,
                        }
                    } else {
                        // Success - create video content with thumbnail and duration
                        content = map[string]interface{}{
                            "type":     "video",
                            "text":     msg.Content,
                            "image":    thumbnailBase64,
                            "duration": duration,
                        }
                    }
                    fileInfo = map[string]interface{}{
                        "fileDescr": map[string]interface{}{
                            "fileDescrComplete": false,
                            "fileDescrPartNo":   0,
                            "fileDescrText":     "",
                        },
                        "fileName": attachment.Filename,
                        "fileSize": attachment.Size,
                    }
                    
                case "voice":
                    // For voice messages, create file attachment
                    content = map[string]interface{}{
                        "text": msg.Content,
                        "type": "file",
                    }
                    fileInfo = map[string]interface{}{
                        "fileDescr": map[string]interface{}{
                            "fileDescrComplete": false,
                            "fileDescrPartNo":   0,
                            "fileDescrText":     "",
                        },
                        "fileName": attachment.Filename,
                        "fileSize": attachment.Size,
                    }
                    
                default: // "file" or unknown
                    // Generic file attachment
                    content = map[string]interface{}{
                        "text": msg.Content,
                        "type": "file",
                    }
                    fileInfo = map[string]interface{}{
                        "fileDescr": map[string]interface{}{
                            "fileDescrComplete": false,
                            "fileDescrPartNo":   0,
                            "fileDescrText":     "",
                        },
                        "fileName": attachment.Filename,
                        "fileSize": attachment.Size,
                    }
                }
            } else {
                // Regular text message
                content = map[string]interface{}{
                    "text": msg.Content,
                    "type": "text",
                }
            }

            // Build params object with correct structure
            params := map[string]interface{}{
                "content": content,
            }

            // Add file info for images
            if fileInfo != nil {
                params["file"] = fileInfo
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
                "message_id":     msgData.MessageID,
                "chat_msg_event": "x.msg.new",
                "shared_msg_id":  msgData.SharedMsgID,
                "msg_body":       msgBodyBytes,
                "msg_sent":       msgSent,
                "created_at":     msg.Timestamp.Format("2006-01-02 15:04:05"),
                "updated_at":     msg.Timestamp.Format("2006-01-02 15:04:05"),
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
                } else if templateRow != nil && len(templateRow) > 0 {
                    rowValues[k] = templateRow[col]
                } else {
                    // Default values for empty database
                    rowValues[k] = nil
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

func bulkInsertChatItems(tx *sql.Tx, data BulkInsertData, jsonDir string, contactID int) error {
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
            
            // Handle file attachments for all message types with attachments
            if len(msg.Attachments) > 0 {
                attachment := msg.Attachments[0]
                _, err := insertFileAttachment(tx, attachment, msgData.ChatItemID, msg.IsSent, jsonDir, msg.MessageType, contactID)
                if err != nil {
                    log.Printf("Warning: failed to create file attachment for %s: %v", attachment.Filename, err)
                    // Continue without file attachment
                }
            }

            var itemSent int
            var itemContentTag string
            var itemStatus string
            if msg.IsSent {
                itemSent = 1
                itemContentTag = "sndMsgContent"
                itemStatus = "snd_rcvd ok complete"
            } else {
                itemSent = 0
                itemContentTag = "rcvMsgContent"
                itemStatus = "rcv_read"
            }

            var msgContent map[string]interface{}

            // Handle different message types with attachments
            if len(msg.Attachments) > 0 {
                attachment := msg.Attachments[0]
                
                switch msg.MessageType {
                case "image":
                    imagePath := filepath.Join(jsonDir, attachment.URL)
                    imageBase64, err := encodeImageToBase64(imagePath)
                    if err != nil {
                        log.Printf("Warning: failed to encode image %s: %v", imagePath, err)
                        // Fallback to text with file info
                        msgContent = map[string]interface{}{
                            "type": "text",
                            "text": fmt.Sprintf("[Image: %s]%s", attachment.Filename,
                                func() string { if msg.Content != "" { return "\n" + msg.Content }; return "" }()),
                        }
                    } else {
                        msgContent = map[string]interface{}{
                            "type":  "image",
                            "text":  msg.Content,
                            "image": imageBase64,
                        }
                    }
                    
                case "video":
                    // For videos, try to generate thumbnail and get duration
                    if len(msg.Attachments) > 0 {
                        attachment := msg.Attachments[0]
                        videoPath := filepath.Join(jsonDir, attachment.URL)
                        thumbnailBase64, duration, err := generateVideoThumbnail(videoPath)
                        if err != nil {
                            log.Printf("Warning: failed to generate video thumbnail for %s: %v", attachment.Filename, err)
                            // Fallback to file type without thumbnail
                            msgContent = map[string]interface{}{
                                "type": "file",
                                "text": msg.Content,
                            }
                        } else {
                            // Success - create video content with thumbnail and duration
                            msgContent = map[string]interface{}{
                                "type":     "video",
                                "text":     msg.Content,
                                "image":    thumbnailBase64,
                                "duration": duration,
                            }
                        }
                    } else {
                        msgContent = map[string]interface{}{
                            "type": "file",
                            "text": msg.Content,
                        }
                    }
                    
                case "voice":
                    // For voice messages, use file type
                    msgContent = map[string]interface{}{
                        "type": "file",
                        "text": msg.Content,
                    }
                    
                default: // "file" or unknown
                    // Generic file attachment
                    msgContent = map[string]interface{}{
                        "type": "file",
                        "text": msg.Content,
                    }
                }
            } else {
                msgContent = map[string]interface{}{
                    "type": "text",
                    "text": msg.Content,
                }
            }

            itemContent := map[string]interface{}{
                itemContentTag: map[string]interface{}{
                    "msgContent": msgContent,
                },
            }

            itemContentBytes, err := json.Marshal(itemContent)
            if err != nil {
                return fmt.Errorf("failed to marshal item_content: %w", err)
            }

            overrideFields := map[string]interface{}{
                "chat_item_id":       msgData.ChatItemID,
                "user_id":            1, // Use the available user ID
                "contact_id":         contactID, // Associate with specified contact
                "created_by_msg_id":  msgData.MessageID,
                "shared_msg_id":      msgData.SharedMsgID,
                "item_content":       string(itemContentBytes),
                "item_text":          msg.Content,
                "item_content_tag":   itemContentTag,
                "item_sent":          itemSent,
                "item_status":        itemStatus,
                "item_deleted":       0, // Not deleted
                "item_edited":        0, // Not edited (prevent edited icon)
                "include_in_history": 1, // Include in history
                "user_mention":       0, // Not a mention
                "show_group_as_sender": 0, // Not a group message
                // "via_proxy":         nil,
                "item_ts":            msg.Timestamp.Format("2006-01-02 15:04:05"),
                "created_at":         msg.Timestamp.Format("2006-01-02 15:04:05"),
                "updated_at":         msg.Timestamp.Format("2006-01-02 15:04:05"),
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

                overrideFields["quoted_shared_msg_id"] = msg.QuotedMessage.SharedMsgID
                overrideFields["quoted_sent_at"] = msg.QuotedMessage.SentAt.Format("2006-01-02 15:04:05")
                overrideFields["quoted_content"] = string(quotedContentBytes)
                overrideFields["quoted_sent"] = quotedSent
            } else {
                overrideFields["quoted_shared_msg_id"] = nil
                overrideFields["quoted_sent_at"] = nil
                overrideFields["quoted_content"] = nil
                overrideFields["quoted_sent"] = nil
            }

            rowValues := make([]interface{}, len(columns))
            for k, col := range columns {
                if val, override := overrideFields[col]; override {
                    rowValues[k] = val
                } else if templateRow != nil && len(templateRow) > 0 {
                    rowValues[k] = templateRow[col]
                } else {
                    // Default values for empty database
                    rowValues[k] = nil
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
                } else if templateRow != nil && len(templateRow) > 0 {
                    rowValues[k] = templateRow[col]
                } else {
                    // Default values for empty database
                    rowValues[k] = nil
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
                "connection_id":   1, // Use first available connection ID
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
                } else if templateRow != nil && len(templateRow) > 0 {
                    rowValues[k] = templateRow[col]
                } else {
                    // Default values for empty database
                    rowValues[k] = nil
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

// Helper function to insert file attachment and return file_id
func insertFileAttachment(tx *sql.Tx, attachment UniversalAttachment, chatItemID int, isSent bool, jsonDir string, messageType string, contactID int) (int, error) {
    filePath := filepath.Join(jsonDir, attachment.URL)
    
    // Check if file exists
    if _, err := os.Stat(filePath); os.IsNotExist(err) {
        return 0, fmt.Errorf("file not found: %s", filePath)
    }
    
    // Get template file row for default values
    templateRow, err := getTemplateRow(tx, "files", "file_id")
    if err != nil {
        return 0, fmt.Errorf("failed to get template file row: %w", err)
    }
    
    // Get next file_id
    var nextFileID int
    err = tx.QueryRow("SELECT COALESCE(MAX(file_id), 0) + 1 FROM files").Scan(&nextFileID)
    if err != nil {
        return 0, fmt.Errorf("failed to get next file_id: %w", err)
    }
    
    // Insert into files table
    columns, err := getTableColumns(tx, "files")
    if err != nil {
        return 0, err
    }
    
    // Copy all files to SimpleX files directory so they are accessible/downloadable
    err = copyFileToSimplexDir(filePath, attachment.Filename)
    if err != nil {
        return 0, fmt.Errorf("failed to copy file to SimpleX directory: %w", err)
    }
    
    // Set file status and protocol based on message type
    var fileStatus string
    var protocol string
    if messageType == "video" {
        // Videos use local storage without transfer records
        fileStatus = "snd_stored"  // Local storage, not transferred
        protocol = "local"         // Local protocol, not smp/xftp
    } else if messageType == "image" || messageType == "voice" {
        // Images and voice use xftp protocol like original SimpleX files
        if isSent {
            fileStatus = "snd_complete"
        } else {
            fileStatus = "rcv_complete"
        }
        protocol = "xftp"
    } else {
        // For other files, use standard transfer status
        if isSent {
            fileStatus = "snd_complete"
        } else {
            fileStatus = "rcv_complete"
        }
        protocol = "smp"
    }
    
    overrideFields := map[string]interface{}{
        "file_id":        nextFileID,
        "contact_id":     contactID, // Associate with specified contact
        "file_name":      attachment.Filename,
        "file_path":      attachment.Filename, // Store just filename like working video
        "file_size":      attachment.Size,
        "chunk_size":     16384, // Standard chunk size
        "user_id":        1, // Use available user ID
        "chat_item_id":   chatItemID,
        "ci_file_status": fileStatus,
        "protocol":       protocol,
        "created_at":     time.Now().Format("2006-01-02 15:04:05"),
        "updated_at":     time.Now().Format("2006-01-02 15:04:05"),
        // Explicitly set encryption fields to NULL for local videos
        "file_crypto_key":   nil,
        "file_crypto_nonce": nil,
    }
    
    rowValues := make([]interface{}, len(columns))
    for i, col := range columns {
        if val, override := overrideFields[col]; override {
            rowValues[i] = val
        } else if templateRow != nil && len(templateRow) > 0 {
            rowValues[i] = templateRow[col]
        } else {
            rowValues[i] = nil
        }
    }
    
    placeholders := "(" + strings.Repeat("?,", len(columns)-1) + "?)"
    query := fmt.Sprintf("INSERT INTO files (%s) VALUES %s",
        strings.Join(columns, ", "), placeholders)
    
    _, err = tx.Exec(query, rowValues...)
    if err != nil {
        return 0, fmt.Errorf("failed to insert file: %w", err)
    }
    
    // Only videos don't need snd_files/rcv_files entries (they use local protocol)
    // Images and voice messages need these entries (they use xftp protocol)
    if messageType != "video" {
        // Insert into snd_files or rcv_files table
        if isSent {
            err = insertSndFile(tx, nextFileID)
        } else {
            err = insertRcvFile(tx, nextFileID)
        }
        if err != nil {
            return 0, fmt.Errorf("failed to insert file transfer record: %w", err)
        }
    }
    
    return nextFileID, nil
}

func insertSndFile(tx *sql.Tx, fileID int) error {
    templateRow, err := getTemplateRow(tx, "snd_files", "file_id")
    if err != nil {
        return err
    }
    
    columns, err := getTableColumns(tx, "snd_files")
    if err != nil {
        return err
    }
    
    // Generate unique last_inline_msg_delivery_id to avoid constraint violations
    var nextDeliveryID int
    err = tx.QueryRow("SELECT COALESCE(MAX(last_inline_msg_delivery_id), 0) + 1 FROM snd_files").Scan(&nextDeliveryID)
    if err != nil {
        return fmt.Errorf("failed to get next delivery ID: %w", err)
    }
    
    overrideFields := map[string]interface{}{
        "file_id":                     fileID,
        "connection_id":               1, // Use available connection
        "file_status":                 "complete",
        "last_inline_msg_delivery_id": nextDeliveryID,
        "created_at":                  time.Now().Format("2006-01-02 15:04:05"),
        "updated_at":                  time.Now().Format("2006-01-02 15:04:05"),
    }
    
    rowValues := make([]interface{}, len(columns))
    for i, col := range columns {
        if val, override := overrideFields[col]; override {
            rowValues[i] = val
        } else if templateRow != nil && len(templateRow) > 0 {
            rowValues[i] = templateRow[col]
        } else {
            rowValues[i] = nil
        }
    }
    
    placeholders := "(" + strings.Repeat("?,", len(columns)-1) + "?)"
    query := fmt.Sprintf("INSERT INTO snd_files (%s) VALUES %s",
        strings.Join(columns, ", "), placeholders)
    
    _, err = tx.Exec(query, rowValues...)
    return err
}

func insertRcvFile(tx *sql.Tx, fileID int) error {
    templateRow, err := getTemplateRow(tx, "rcv_files", "file_id")
    if err != nil {
        return err
    }
    
    columns, err := getTableColumns(tx, "rcv_files")
    if err != nil {
        return err
    }
    
    overrideFields := map[string]interface{}{
        "file_id":                fileID,
        "file_status":            "complete",
        "user_approved_relays":   0, // Set to 0 for imported files
        "created_at":             time.Now().Format("2006-01-02 15:04:05"),
        "updated_at":             time.Now().Format("2006-01-02 15:04:05"),
    }
    
    rowValues := make([]interface{}, len(columns))
    for i, col := range columns {
        if val, override := overrideFields[col]; override {
            rowValues[i] = val
        } else if templateRow != nil && len(templateRow) > 0 {
            rowValues[i] = templateRow[col]
        } else {
            rowValues[i] = nil
        }
    }
    
    placeholders := "(" + strings.Repeat("?,", len(columns)-1) + "?)"
    query := fmt.Sprintf("INSERT INTO rcv_files (%s) VALUES %s",
        strings.Join(columns, ", "), placeholders)
    
    _, err = tx.Exec(query, rowValues...)
    return err
}

func bulkInsertUniversalMessages(db *sql.DB, messages []UniversalMessage, startMessageID int, jsonDir string, contactID int) error {
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

    err = bulkInsertMessages(tx, bulkData, jsonDir, contactID)
    if err != nil {
        return fmt.Errorf("failed to bulk insert messages: %w", err)
    }

    err = bulkInsertChatItems(tx, bulkData, jsonDir, contactID)
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
    var contactName string
    batchSize := 500 // Hardcoded batch size

    flag.StringVar(&jsonFilePath, "json", "", "Path to Discord JSON export file (required)")
    flag.StringVar(&myUsername, "me", "", "Your Discord username to identify sent messages (required)")
    flag.StringVar(&contactName, "contact", "", "SimpleX contact name to import messages to (required)")
    flag.StringVar(&dbPath, "db", "", "Path to SQLCipher database (required)")
    flag.Parse()

    if jsonFilePath == "" {
        log.Fatal("JSON file path is required. Use -json flag.")
    }
    if myUsername == "" {
        log.Fatal("Username is required. Use -me flag.")
    }
    if contactName == "" {
        log.Fatal("Contact name is required. Use -contact flag.")
    }
    if dbPath == "" {
        log.Fatal("Database path is required. Use -db flag.")
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

    // Look up contact ID by name
    contactID, err := getContactIDByName(db, contactName)
    if err != nil {
        log.Fatalf("Failed to find contact '%s': %v", contactName, err)
    }
    fmt.Printf("Contact: %s (ID: %d)\n", contactName, contactID)

    // Get starting message ID
    var startMessageID int
    err = db.QueryRow("SELECT COALESCE(MAX(message_id), 0) + 1 FROM messages").Scan(&startMessageID)
    if err != nil {
        log.Fatalf("Failed to get starting message ID: %v", err)
    }

    fmt.Printf("Starting message ID: %d\n", startMessageID)

    // Get directory containing the JSON file for relative path resolution
    jsonDir := filepath.Dir(jsonFilePath)
    fmt.Printf("JSON directory: %s\n", jsonDir)

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
        universalMsg := ConvertDiscordMessage(discordMsg, myUsername, discordToSharedMsgID, discordMessages, jsonDir)
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

        err = bulkInsertUniversalMessages(db, batch, batchStartID, jsonDir, contactID)
        if err != nil {
            log.Fatalf("Failed to insert batch %d-%d: %v", i+1, end, err)
        }

        fmt.Printf("Successfully inserted batch %d-%d\n", i+1, end)
    }
}
