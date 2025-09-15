# Discord to SimpleX Chat Importer

Import your Discord chat history into SimpleX Chat with full support for text messages, images, videos, and voice messages.
Only works for DMs.

**DISCLAIMER:** This tool was almost entirely vibe-coded using claude-code with Sonnet 4. It looks to work alright. Still
a good idea to do a human-go-through of the codebase properly sometime..

## Features

- **Complete message import**: Text, images, videos, voice messages, and file attachments
- **Discord reaction support**: Imports all Discord reactions (emoji reactions show up correctly for SimpleX's 6 supported emojis: 👍, 🚀, ❤, ✅, 😀, 😢; other emojis display as "?" but are still imported)
- **Video thumbnails**: Automatically generates thumbnails for imported videos using FFmpeg
- **Downloadable attachments**: Images, videos, and voice messages are properly saved and accessible in SimpleX
- **Contact mapping**: Import messages to any existing SimpleX contact
- **Message threading**: Preserves Discord reply structure
- **Batch processing**: Efficient bulk import with using pre-configured batch sizes
- **SQLCipher support**: Works with encrypted SimpleX databases

## Prerequisites

- [Nix](https://nixos.org/download.html) package manager
- SimpleX Chat with an existing database and have initiated chat on SimpleX with your corresponding Discord contact
- Discord chat JSON export from [discord-chat-exporter](https://github.com/Tyrrrz/DiscordChatExporter)

## Installation

1. Clone this repository:
```bash
git clone https://github.com/ritiek/discord-to-simplex
cd discord-to-simplex
```

2. Enter the Nix development environment:
```bash
nix develop
```

**TODO:** Add more installation methods.

## Usage

**Make sure to export your SimpleX Chat database and back it up before proceeding!**

### Step 1: Export Discord Chat

Use [discord-chat-exporter](https://github.com/Tyrrrz/DiscordChatExporter) to export your Discord DM or channel:

```bash
# Export a Discord DM (replace TOKEN and CHANNEL_ID)
DiscordChatExporter.Cli export --channel CHANNEL_ID --token "TOKEN" --output . --media --reuse-media --markdown false --format Json
```

This will create a JSON file and a `_Files` folder containing all attachments.

### Step 2: Export SimpleX Chat Database

Export your SimpleX Chat database to a ZIP file using the SimpleX app:

1. Open SimpleX Chat
2. Go to Settings > Database
3. Choose "Export database" 
4. Save the ZIP file containing your database and files

### Step 3: Find Your SimpleX Contact

Identify the contact name you want to import messages to. You can check your SimpleX contacts in the app.

### Step 4: Prepare Database Password

You can provide your SimpleX database password in two ways:

**Option A: Environment variable (recommended for scripts):**
```bash
export SQLCIPHER_KEY='your-simplex-database-password'
```

**Option B: Interactive prompt (will be prompted if environment variable not set):**
The tool will securely prompt for your password if the `SQLCIPHER_KEY` environment variable is not set.

### Step 5: Run the Import

```bash
go run main.go \
  -json ./path/to/your-discord-export.json \
  -me "YourDiscordUsername" \
  -contact "FriendSimpleXContactUserName" \
  -zip /path/to/simplex-export.zip \
  -output ./updated-simplex-export.zip
```

**Parameters:**
- `-json`: Path to the Discord export JSON file
- `-me`: Your Discord username (to distinguish sent vs received messages)
- `-contact`: SimpleX contact name to import messages to
- `-zip`: Path to your SimpleX export ZIP file
- `-output`: Path for the updated SimpleX ZIP file (optional, defaults to input with '_updated' suffix)

### Step 6: Import Back to SimpleX

Import the updated ZIP file back into SimpleX Chat:

1. Open SimpleX Chat
2. Go to Settings > Database
3. Choose "Import database"
4. Select the updated ZIP file created by the tool

### Examples

**With environment variable:**
```bash
export SQLCIPHER_KEY='my-secret-password'
go run main.go \
  -json ./discord-export.json \
  -me "john_doe" \
  -contact "alice" \
  -zip ./simplex-export.zip \
  -output ./simplex-with-discord-messages.zip
```

**With interactive password prompt:**
```bash
go run main.go \
  -json ./discord-export.json \
  -me "john_doe" \
  -contact "alice" \
  -zip ./simplex-export.zip \
  -output ./simplex-with-discord-messages.zip
# Tool will prompt: "Enter SimpleX database password: " (password hidden)
```

## Version

Tested on the following version of SimpleX Chat:
```
App version: v6.4.4
App build: 119
Core version: v6.4.4.2
simplexmq: v6.4.4.1 ( )
```

## How It Works

1. **Extracts SimpleX ZIP export** to a temporary directory
2. **Parses Discord JSON export** and extracts messages, attachments, reactions, and metadata
3. **Imports Discord reactions** with proper emoji normalization for SimpleX compatibility
4. **Generates video thumbnails** using FFmpeg for proper display in SimpleX
5. **Copies attachments** to the extracted SimpleX files directory for accessibility
6. **Maps Discord users** to SimpleX contacts based on your specification
7. **Bulk inserts** messages and reactions into the extracted SimpleX database with proper relationships
8. **Preserves message order** and reply threading from Discord
9. **Creates updated ZIP export** with all imported messages and files ready for SimpleX import

## Supported File Types

- **Images**: JPG, PNG, GIF, WEBP
- **Videos**: MP4, MOV, AVI, WEBM (with thumbnail generation)
- **Audio**: MP3, WAV, OGG, M4A (voice messages)
- **Files**: All other file types as downloadable attachments

## Database Structure

The importer creates proper SimpleX database entries:
- Messages in `messages` and `chat_items` tables
- Discord reactions in `chat_item_reactions` table with proper emoji normalization
- File attachments in `files`, `snd_files`, `rcv_files` tables
- Proper contact associations and message threading
- Compatible with SimpleX's encryption and sync features

## License

MIT
