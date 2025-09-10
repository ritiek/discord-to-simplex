# Discord to SimpleX Chat Importer

Import your Discord chat history into SimpleX Chat with full support for text messages, images, videos, and voice messages.
Only works for DMs.

**DISCLAIMER:** This tool was almost entirely vibe-coded using claude-code with Sonnet 4. It looks to work alright. Still
a good idea to do a human-go-through of the codebase properly sometime..

## Features

- **Complete message import**: Text, images, videos, voice messages, and file attachments
- **Video thumbnails**: Automatically generates thumbnails for imported videos using FFmpeg
- **Downloadable attachments**: Images, videos, and voice messages are properly saved and accessible in SimpleX
- **Contact mapping**: Import messages to any existing SimpleX contact
- **Message threading**: Preserves Discord reply structure
- **Batch processing**: Efficient bulk import with configurable batch sizes
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

**Make sure to take the latest export of your SimpleX Chat DB and back it up before proceeding!**

### Step 1: Export Discord Chat

Use [discord-chat-exporter](https://github.com/Tyrrrz/DiscordChatExporter) to export your Discord DM or channel:

```bash
# Export a Discord DM (replace TOKEN and CHANNEL_ID)
DiscordChatExporter.Cli export --channel CHANNEL_ID --token "TOKEN" --output . --media --reuse-media --markdown false --format Json
```

This will create a JSON file and a `_Files` folder containing all attachments.

### Step 2: Find Your SimpleX Contact

First, identify the contact name you want to import messages to. You can check your SimpleX contacts in the app or database.

### Step 3: Set Database Password

Export your SimpleX database password as an environment variable:

```bash
export SQLCIPHER_KEY='your-simplex-database-password'
```

### Step 4: Run the Import

```bash
go run main.go \
  -json ./path/to/your-export.json \
  -me "YourDiscordUsername" \
  -contact "FriendSimpleXContactUserName" \
  -db /path/to/simplex/simplex_v1_chat.db
```

**Parameters:**
- `-json`: Path to the Discord export JSON file
- `-me`: Your Discord username (to distinguish sent vs received messages)
- `-contact`: SimpleX contact name to import messages to
- `-db`: Path to your SimpleX chat database

### Example

```bash
export SQLCIPHER_KEY='my-secret-password'
go run main.go \
  -json ./discord-export.json \
  -me "john_doe" \
  -contact "alice" \
  -db ~/.local/share/simplex/simplex_v1_chat.db
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

1. **Parses Discord JSON export** and extracts messages, attachments, and metadata
2. **Generates video thumbnails** using FFmpeg for proper display in SimpleX
3. **Copies attachments** to SimpleX files directory for accessibility
4. **Maps Discord users** to SimpleX contacts based on your specification
5. **Bulk inserts** messages into SimpleX database with proper relationships
6. **Preserves message order** and reply threading from Discord

## Supported File Types

- **Images**: JPG, PNG, GIF, WEBP
- **Videos**: MP4, MOV, AVI, WEBM (with thumbnail generation)
- **Audio**: MP3, WAV, OGG, M4A (voice messages)
- **Files**: All other file types as downloadable attachments

## Database Structure

The importer creates proper SimpleX database entries:
- Messages in `messages` and `chat_items` tables
- File attachments in `files`, `snd_files`, `rcv_files` tables
- Proper contact associations and message threading
- Compatible with SimpleX's encryption and sync features

## License

MIT
