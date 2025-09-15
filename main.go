package main

import (
    "archive/zip"
    "bufio"
    "database/sql"
    "encoding/base64"
    "encoding/json"
    "path/filepath"
    "flag"
    "fmt"
    "io"
    "log"
    "os"
    "os/exec"
    "strconv"
    "strings"
    "syscall"
    "time"

    "golang.org/x/term"
    _ "github.com/xeodou/go-sqlcipher"
)

// Discord JSON export structure
type DiscordExport struct {
    Channel  struct {
        Name string `json:"name"`
    } `json:"channel"`
    Messages []DiscordMessage `json:"messages"`
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

    // Quote information for Discord replies
    QuotedMessage *QuotedMessage `json:"quotedMessage,omitempty"`

    // Platform-specific data (stored as JSON for flexibility)
    PlatformData map[string]interface{} `json:"platformData,omitempty"`

    // Message state
    IsPinned    bool `json:"isPinned"`
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

type DiscordReaction struct {
    Emoji DiscordEmoji     `json:"emoji"`
    Count int              `json:"count"`
    Users []DiscordAuthor  `json:"users"`
}

type DiscordEmoji struct {
    ID         string `json:"id"`
    Name       string `json:"name"`
    Code       string `json:"code"`
    IsAnimated bool   `json:"isAnimated"`
    ImageURL   string `json:"imageUrl"`
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
    imageData, err := os.ReadFile(imagePath)
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
    thumbnailData, err := os.ReadFile(thumbnailPath)
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

// Prompt for SimpleX database password securely
func promptForPassword() (string, error) {
    fmt.Print("Enter SimpleX database password: ")

    // Check if we're running in a terminal
    if term.IsTerminal(int(syscall.Stdin)) {
        // Use secure password input (no echo)
        passwordBytes, err := term.ReadPassword(int(syscall.Stdin))
        fmt.Println() // Print newline after password input
        if err != nil {
            return "", fmt.Errorf("failed to read password: %w", err)
        }
        return string(passwordBytes), nil
    } else {
        // Fallback for non-terminal input (testing, pipes, etc.)
        reader := bufio.NewReader(os.Stdin)
        password, err := reader.ReadString('\n')
        if err != nil {
            return "", fmt.Errorf("failed to read password: %w", err)
        }
        return strings.TrimSpace(password), nil
    }
}

// Extract SimpleX ZIP export to temporary directory
func extractSimplexZip(zipPath string) (string, error) {
    // Create temporary directory
    tempDir, err := os.MkdirTemp("", "simplex_import_")
    if err != nil {
        return "", fmt.Errorf("failed to create temp directory: %w", err)
    }

    // Open ZIP file
    r, err := zip.OpenReader(zipPath)
    if err != nil {
        os.RemoveAll(tempDir)
        return "", fmt.Errorf("failed to open ZIP file: %w", err)
    }
    defer r.Close()

    // Extract files
    for _, f := range r.File {
        rc, err := f.Open()
        if err != nil {
            os.RemoveAll(tempDir)
            return "", fmt.Errorf("failed to open file in ZIP: %w", err)
        }

        path := filepath.Join(tempDir, f.Name)

        if f.FileInfo().IsDir() {
            os.MkdirAll(path, f.FileInfo().Mode())
            rc.Close()
            continue
        }

        // Create directory if needed
        if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
            rc.Close()
            os.RemoveAll(tempDir)
            return "", fmt.Errorf("failed to create directory: %w", err)
        }

        // Extract file
        outFile, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.FileInfo().Mode())
        if err != nil {
            rc.Close()
            os.RemoveAll(tempDir)
            return "", fmt.Errorf("failed to create output file: %w", err)
        }

        _, err = io.Copy(outFile, rc)
        outFile.Close()
        rc.Close()

        if err != nil {
            os.RemoveAll(tempDir)
            return "", fmt.Errorf("failed to extract file: %w", err)
        }
    }

    return tempDir, nil
}

// Create new SimpleX ZIP export from directory
func createSimplexZip(sourceDir, outputZipPath string) error {
    // Create output ZIP file
    zipFile, err := os.Create(outputZipPath)
    if err != nil {
        return fmt.Errorf("failed to create ZIP file: %w", err)
    }
    defer zipFile.Close()

    zipWriter := zip.NewWriter(zipFile)
    defer zipWriter.Close()

    // Walk through source directory
    err = filepath.Walk(sourceDir, func(filePath string, info os.FileInfo, err error) error {
        if err != nil {
            return err
        }

        // Get relative path from source directory
        relPath, err := filepath.Rel(sourceDir, filePath)
        if err != nil {
            return err
        }

        // Skip root directory itself
        if relPath == "." {
            return nil
        }

        // Create header
        header, err := zip.FileInfoHeader(info)
        if err != nil {
            return err
        }
        header.Name = relPath

        if info.IsDir() {
            header.Name += "/"
        } else {
            header.Method = zip.Deflate
        }

        // Create file in ZIP
        writer, err := zipWriter.CreateHeader(header)
        if err != nil {
            return err
        }

        if !info.IsDir() {
            // Copy file content
            file, err := os.Open(filePath)
            if err != nil {
                return err
            }
            defer file.Close()

            _, err = io.Copy(writer, file)
            if err != nil {
                return err
            }
        }

        return nil
    })

    return err
}

// Find SimpleX database file in extracted directory
func findSimplexDB(extractedDir string) (string, error) {
    var dbPath string

    err := filepath.Walk(extractedDir, func(path string, info os.FileInfo, err error) error {
        if err != nil {
            return err
        }

        if !info.IsDir() && (strings.Contains(info.Name(), "simplex_v1_chat.db") || strings.Contains(info.Name(), "chat.db")) {
            dbPath = path
            return filepath.SkipDir // Found it, stop walking
        }

        return nil
    })

    if err != nil {
        return "", fmt.Errorf("failed to search for database: %w", err)
    }

    if dbPath == "" {
        return "", fmt.Errorf("no SimpleX database found in ZIP")
    }

    return dbPath, nil
}

// Find or create SimpleX files directory in extracted directory
func findOrCreateSimplexFilesDir(extractedDir string) (string, error) {
    var filesDir string

    err := filepath.Walk(extractedDir, func(path string, info os.FileInfo, err error) error {
        if err != nil {
            return err
        }

        if info.IsDir() && (strings.Contains(info.Name(), "simplex_v1_files") || strings.Contains(info.Name(), "files")) {
            filesDir = path
            return filepath.SkipDir // Found it, stop walking
        }

        return nil
    })

    if err != nil {
        return "", fmt.Errorf("failed to search for files directory: %w", err)
    }

    // If not found, create it
    if filesDir == "" {
        filesDir = filepath.Join(extractedDir, "simplex_v1_files")
        if err := os.MkdirAll(filesDir, 0755); err != nil {
            return "", fmt.Errorf("failed to create files directory: %w", err)
        }
    }

    return filesDir, nil
}

// Helper function to copy video file to SimpleX files directory
func copyFileToSimplexDir(sourcePath, filename, simplexFilesDir string) error {
    // Ensure SimpleX files directory exists
    if err := os.MkdirAll(simplexFilesDir, 0755); err != nil {
        return fmt.Errorf("failed to create SimpleX files directory: %w", err)
    }

    // Truncate filename if too long (filesystem limit is usually 255 chars)
    if len(filename) > 200 {
        ext := filepath.Ext(filename)
        baseName := filename[:200-len(ext)]
        filename = baseName + ext
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

    // Convert reactions
    var reactions []UniversalReaction
    for _, react := range discordMsg.Reactions {
        if reactMap, ok := react.(map[string]interface{}); ok {
            if emojiMap, ok := reactMap["emoji"].(map[string]interface{}); ok {
                emoji := fmt.Sprintf("%v", emojiMap["name"])
                count := int(reactMap["count"].(float64))

                var userIDs []string
                if users, ok := reactMap["users"].([]interface{}); ok {
                    for _, user := range users {
                        if userMap, ok := user.(map[string]interface{}); ok {
                            userIDs = append(userIDs, fmt.Sprintf("%v", userMap["id"]))
                        }
                    }
                }

                reactions = append(reactions, UniversalReaction{
                    Emoji:   emoji,
                    Count:   count,
                    UserIDs: userIDs,
                })
            }
        }
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
        Reactions: reactions,
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

func bulkInsertChatItems(tx *sql.Tx, data BulkInsertData, jsonDir string, contactID int, simplexFilesDir string) error {
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
                _, err := insertFileAttachment(tx, attachment, msgData.ChatItemID, msg.IsSent, jsonDir, msg.MessageType, contactID, simplexFilesDir)
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
func insertFileAttachment(tx *sql.Tx, attachment UniversalAttachment, chatItemID int, isSent bool, jsonDir string, messageType string, contactID int, simplexFilesDir string) (int, error) {
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

    // Truncate filename if too long (same logic as copyFileToSimplexDir)
    truncatedFilename := attachment.Filename
    if len(truncatedFilename) > 200 {
        ext := filepath.Ext(truncatedFilename)
        baseName := truncatedFilename[:200-len(ext)]
        truncatedFilename = baseName + ext
    }

    // Copy all files to SimpleX files directory so they are accessible/downloadable
    err = copyFileToSimplexDir(filePath, attachment.Filename, simplexFilesDir)
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
        "file_name":      truncatedFilename, // Use truncated filename
        "file_path":      truncatedFilename, // Store truncated filename like working video
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

func normalizeEmojiForSimpleX(emoji string) string {
    // Remove variation selectors (U+FE0E, U+FE0F) and other modifiers that Discord adds
    // but SimpleX doesn't support
    normalized := ""
    for _, r := range emoji {
        // Skip variation selectors and other modifier characters
        if r != '\uFE0E' && r != '\uFE0F' {
            normalized += string(r)
        }
    }
    return normalized
}

func bulkInsertReactions(tx *sql.Tx, data BulkInsertData, contactID int) error {
    // Get the next available reaction ID
    var nextReactionID int
    err := tx.QueryRow("SELECT COALESCE(MAX(chat_item_reaction_id), 0) + 1 FROM chat_item_reactions").Scan(&nextReactionID)
    if err != nil {
        return fmt.Errorf("failed to get next reaction ID: %w", err)
    }

    reactionIDCounter := nextReactionID

    for _, msgData := range data.Messages {
        msg := msgData.Message

        for _, reaction := range msg.Reactions {
            // Normalize emoji by removing variation selectors for SimpleX compatibility
            normalizedEmoji := normalizeEmojiForSimpleX(reaction.Emoji)

            // Create SimpleX format reaction JSON
            reactionJSON := fmt.Sprintf(`{"type":"emoji","emoji":"%s"}`, normalizedEmoji)

            // In SimpleX, reactions need to track who made the reaction
            // Since we're importing from Discord where we don't have individual reaction senders,
            // we'll assume the contact reacted to our sent messages and we reacted to their messages
            var reactionSent int
            var actualContactID interface{}
            if msg.IsSent {
                // If we sent the message, the contact reacted to it
                reactionSent = 0
                actualContactID = contactID
            } else {
                // If the contact sent the message, we reacted to it
                reactionSent = 1
                actualContactID = contactID // User reactions also need the contact_id
            }

            // Insert reaction
            _, err = tx.Exec(`
                INSERT INTO chat_item_reactions (
                    chat_item_reaction_id,
                    shared_msg_id,
                    contact_id,
                    created_by_msg_id,
                    reaction,
                    reaction_sent,
                    reaction_ts,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            `, reactionIDCounter, msgData.SharedMsgID, actualContactID, nil, reactionJSON, reactionSent, msg.Timestamp.Format("2006-01-02 15:04:05.000000000"), msg.Timestamp.Format("2006-01-02 15:04:05"), msg.Timestamp.Format("2006-01-02 15:04:05"))

            if err != nil {
                return fmt.Errorf("failed to insert reaction: %w", err)
            }

            reactionIDCounter++
        }
    }

    return nil
}

func bulkInsertUniversalMessages(db *sql.DB, messages []UniversalMessage, startMessageID int, jsonDir string, contactID int, simplexFilesDir string) error {
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

    err = bulkInsertChatItems(tx, bulkData, jsonDir, contactID, simplexFilesDir)
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

    err = bulkInsertReactions(tx, bulkData, contactID)
    if err != nil {
        return fmt.Errorf("failed to bulk insert reactions: %w", err)
    }

    // Commit transaction
    err = tx.Commit()
    if err != nil {
        return fmt.Errorf("failed to commit transaction: %w", err)
    }

    return nil
}

func loadDiscordExport(filePath string) (*DiscordExport, error) {
    data, err := os.ReadFile(filePath)
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
    var zipPath string
    var outputZipPath string
    var contactName string
    batchSize := 500 // Hardcoded batch size

    flag.StringVar(&jsonFilePath, "json", "", "Path to Discord JSON export file (required)")
    flag.StringVar(&myUsername, "me", "", "Your Discord username to identify sent messages (required)")
    flag.StringVar(&contactName, "contact", "", "SimpleX contact name to import messages to (required)")
    flag.StringVar(&zipPath, "zip", "", "Path to SimpleX export ZIP file (required)")
    flag.StringVar(&outputZipPath, "output", "", "Path for output SimpleX ZIP file (optional, defaults to input with '_updated' suffix)")
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
    if zipPath == "" {
        log.Fatal("SimpleX ZIP file path is required. Use -zip flag.")
    }

    // Set default output path if not provided
    if outputZipPath == "" {
        dir := filepath.Dir(zipPath)
        base := filepath.Base(zipPath)
        ext := filepath.Ext(base)
        name := base[:len(base)-len(ext)]
        outputZipPath = filepath.Join(dir, name+"_updated"+ext)
    }

    // Get database password from environment or prompt user
    password := os.Getenv("SQLCIPHER_KEY")
    if password == "" {
        fmt.Println("SQLCIPHER_KEY environment variable not set.")
        var err error
        password, err = promptForPassword()
        if err != nil {
            log.Fatalf("Failed to get database password: %v", err)
        }
        if password == "" {
            log.Fatal("Database password is required")
        }
    }

    // Extract SimpleX ZIP export
    fmt.Printf("Extracting SimpleX ZIP export from: %s\n", zipPath)
    extractedDir, err := extractSimplexZip(zipPath)
    if err != nil {
        log.Fatalf("Failed to extract SimpleX ZIP: %v", err)
    }
    defer os.RemoveAll(extractedDir) // Clean up temporary directory

    // Find database and files directory in extracted content
    dbPath, err := findSimplexDB(extractedDir)
    if err != nil {
        log.Fatalf("Failed to find SimpleX database: %v", err)
    }

    simplexFilesDir, err := findOrCreateSimplexFilesDir(extractedDir)
    if err != nil {
        log.Fatalf("Failed to find or create SimpleX files directory: %v", err)
    }

    fmt.Printf("Found database at: %s\n", dbPath)
    fmt.Printf("Using files directory: %s\n", simplexFilesDir)

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

        err = bulkInsertUniversalMessages(db, batch, batchStartID, jsonDir, contactID, simplexFilesDir)
        if err != nil {
            log.Fatalf("Failed to insert batch %d-%d: %v", i+1, end, err)
        }

        fmt.Printf("Successfully inserted batch %d-%d\n", i+1, end)
    }

    // Close database connection before creating ZIP
    db.Close()

    // Create output ZIP with updated database and files
    fmt.Printf("Creating updated SimpleX ZIP export: %s\n", outputZipPath)
    err = createSimplexZip(extractedDir, outputZipPath)
    if err != nil {
        log.Fatalf("Failed to create output ZIP: %v", err)
    }

    fmt.Printf("Successfully created updated SimpleX export: %s\n", outputZipPath)
    fmt.Printf("Import complete! You can now import this ZIP file back into SimpleX Chat.\n")
}
