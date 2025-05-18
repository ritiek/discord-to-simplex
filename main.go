package main

import (
    "database/sql"
    "fmt"
    "log"
    "os"
    "strings"
		"encoding/base64"
		"encoding/json"

    _ "github.com/xeodou/go-sqlcipher"
)

func main() {
    fmt.Println("Starting clone process...")

    password := os.Getenv("SQLCIPHER_KEY")
    if password == "" {
        log.Fatal("SQLCIPHER_KEY environment variable not set")
    }

    databaseFile := "/home/ritiek/.local/share/simplex/simplex_v1_chat.db"
    dsn := fmt.Sprintf("%s?_key=%s", databaseFile, password)

    db, err := sql.Open("sqlite3", dsn)
    if err != nil {
        log.Fatalf("Failed to open database: %v", err)
    }
    defer db.Close()

    fmt.Println("")

    tableName := "chat_items"
    overrideFields := map[string]interface{}{
        "chat_item_id":      12,
				"shared_msg_id": []byte("tryinghardIFe4SJ"),
        "created_by_msg_id": 7,
    		"item_content": "{\"sndMsgContent\":{\"msgContent\":{\"type\":\"text\",\"text\":\"IT WORKED!!!!!!!!!!!!\"}}}",
    		"item_text": "IT WORKED!!!!!!!!!!!!",
    }

    // Step 1: Delete existing row in chat_items
    deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE chat_item_id = ?", tableName)
    _, err = db.Exec(deleteQuery, overrideFields["chat_item_id"])
    if err != nil {
        log.Fatalf("Failed to delete existing chat_item_id=%v: %v", overrideFields["chat_item_id"], err)
    }
    fmt.Println("Successfully deleted chat_item_id =", overrideFields["chat_item_id"])

    deleteQuery = "DELETE FROM messages WHERE message_id = ?"
    _, err = db.Exec(deleteQuery, 7)
    if err != nil {
        log.Fatalf("Failed to delete existing message_id=7: %v", err)
    }
    fmt.Println("Successfully deleted message_id = 7")

    deleteQuery = "DELETE FROM chat_item_messages WHERE chat_item_id = ?"
    _, err = db.Exec(deleteQuery, 12)
    if err != nil {
        log.Fatalf("Failed to delete existing chat_item_id=12: %v", err)
    }
    fmt.Println("Successfully deleted chat_item_id = 12")

    deleteQuery = "DELETE FROM msg_deliveries WHERE msg_delivery_id = ?"
    _, err = db.Exec(deleteQuery, 7)
    if err != nil {
        log.Fatalf("Failed to delete existing msg_delivery_id=7: %v", err)
    }
    fmt.Println("Successfully deleted chat_item_message_id = 7")

    err = cloneRow(db, "msg_deliveries", "msg_delivery_id", 6, map[string]interface{}{
        "msg_delivery_id": 7,
        "message_id": 7,
				"agent_msg_id": 13,
    })
    if err != nil {
        log.Fatalf("Failed to clone message row: %v", err)
    }
    fmt.Println("✅ Cloned msg delivery row from 6 to 7")

		encoded_msg_id := base64.StdEncoding.EncodeToString([]byte("tryinghardIFe4SJ"))
    fmt.Println(encoded_msg_id)
		msg_body := map[string]interface{}{
			"v": "1-14",
			"msgId": encoded_msg_id,
			"event": "x.msg.new",
			"params": map[string]interface{}{
				"content": map[string]interface{}{
					"text": "IT WORKED!!!!!!!!!!!!",
					"type": "text",
				},
			},
		}

		msgBodyBytes, err := json.Marshal(msg_body)
		if err != nil {
			log.Fatalf("Failed to marshal msg_body: %v", err)
		}

    err = cloneRow(db, "messages", "message_id", 6, map[string]interface{}{
        "message_id": 7,
        "shared_msg_id": []byte("tryinghardIFe4SJ"),
				"msg_body": msgBodyBytes,
    })
    if err != nil {
        log.Fatalf("Failed to clone message row: %v", err)
    }
    fmt.Println("✅ Cloned message row from 6 to 7")

    err = cloneRow(db, "chat_item_messages", "chat_item_id", 11, map[string]interface{}{
        "rowid": 6,
        "chat_item_id": 12,
        "message_id": 7,
    })
    if err != nil {
        log.Fatalf("Failed to clone chat_item_message row: %v", err)
    }
    fmt.Println("✅ Cloned chat_item_message row from 11 to 12")

    colQuery := fmt.Sprintf("PRAGMA table_info(%s);", tableName)
    rows, err := db.Query(colQuery)
    if err != nil {
        log.Fatalf("Failed to query table info: %v", err)
    }
    defer rows.Close()

    var insertCols, selectCols []string
    var insertArgs []interface{}

    for rows.Next() {
        var cid int
        var name, ctype string
        var notnull, pk int
        var dfltValue sql.NullString

        err = rows.Scan(&cid, &name, &ctype, &notnull, &dfltValue, &pk)
        if err != nil {
            log.Fatalf("Failed to scan table info: %v", err)
        }

        if val, override := overrideFields[name]; override {
            insertCols = append(insertCols, name)
            selectCols = append(selectCols, "?")
            insertArgs = append(insertArgs, val)
        } else {
            insertCols = append(insertCols, name)
            selectCols = append(selectCols, name)
        }
    }

    if len(insertCols) == 0 {
        log.Fatal("No insertable columns found.")
    }

    insertColsStr := strings.Join(insertCols, ", ")
    selectColsStr := strings.Join(selectCols, ", ")

    insertQuery := fmt.Sprintf(`
        INSERT INTO %s (%s)
        SELECT %s FROM %s WHERE chat_item_id = ?;
    `, tableName, insertColsStr, selectColsStr, tableName)

    sourceID := 11
    insertArgs = append(insertArgs, sourceID)

    _, err = db.Exec(insertQuery, insertArgs...)
    if err != nil {
        log.Fatalf("Failed to clone row: %v", err)
    }

    fmt.Printf("✅ Successfully cloned chat_item_id from %d to %v\n", sourceID, overrideFields["chat_item_id"])

    fmt.Println("")
}

func cloneRow(db *sql.DB, tableName, primaryKey string, sourceID interface{}, overrideFields map[string]interface{}) error {
    if newID, ok := overrideFields[primaryKey]; ok {
        deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", tableName, primaryKey)
        _, err := db.Exec(deleteQuery, newID)
        if err != nil {
            return fmt.Errorf("delete failed: %w", err)
        }
        fmt.Printf("Deleted existing row with %s = %v from %s\n", primaryKey, newID, tableName)
    }

    colQuery := fmt.Sprintf("PRAGMA table_info(%s);", tableName)
    rows, err := db.Query(colQuery)
    if err != nil {
        return fmt.Errorf("failed to query table info: %w", err)
    }
    defer rows.Close()

    var insertCols, selectCols []string
    var insertArgs []interface{}

    for rows.Next() {
        var cid int
        var name, ctype string
        var notnull, pk int
        var dfltValue sql.NullString

        err = rows.Scan(&cid, &name, &ctype, &notnull, &dfltValue, &pk)
        if err != nil {
            return fmt.Errorf("failed to scan table info: %w", err)
        }

        if val, override := overrideFields[name]; override {
            insertCols = append(insertCols, name)
            selectCols = append(selectCols, "?")
            insertArgs = append(insertArgs, val)
        } else {
            insertCols = append(insertCols, name)
            selectCols = append(selectCols, name)
        }
    }

    if len(insertCols) == 0 {
        return fmt.Errorf("no columns found to insert")
    }

    insertColsStr := strings.Join(insertCols, ", ")
    selectColsStr := strings.Join(selectCols, ", ")

    insertQuery := fmt.Sprintf(`
        INSERT INTO %s (%s)
        SELECT %s FROM %s WHERE %s = ?;
    `, tableName, insertColsStr, selectColsStr, tableName, primaryKey)

    insertArgs = append(insertArgs, sourceID)

    _, err = db.Exec(insertQuery, insertArgs...)
    if err != nil {
        return fmt.Errorf("insert failed: %w", err)
    }

    return nil
}



//
// import (
//     "database/sql"
//     "fmt"
//     "log"
//     "os"
//
//     _ "github.com/xeodou/go-sqlcipher"
// )
//
// func main() {
// 		fmt.Println("Meoweow");
//     password := os.Getenv("SQLCIPHER_KEY")
//     if password == "" {
//         log.Fatal("SQLCIPHER_KEY environment variable not set")
//     }
//
//     databaseFile := "/home/ritiek/Downloads/simplex_v1_chat.db"
//     dsn := fmt.Sprintf("%s?_key=%s", databaseFile, password)
//
//     db, err := sql.Open("sqlite3", dsn)
//     if err != nil {
//         log.Fatalf("Failed to open database: %v", err)
//     }
//
//     defer db.Close()
//     rows, err := db.Query("SELECT * FROM users;")
//     if err != nil {
//         log.Fatal("Error executing query: ", err)
//     }
//     defer rows.Close()
//
//     columns, err := rows.Columns()
//     if err != nil {
//         log.Fatal(err)
//     }
//
//     // Iterate over the rows
//     for rows.Next() {
//         // Create a slice of empty interfaces to hold the column values
//         values := make([]interface{}, len(columns))
//
//         // Create pointers for each value so that we can scan the data into the slice
//         for i := range values {
//             values[i] = new(interface{})
//         }
//
//         // Scan the row into the slice of interfaces
//         if err := rows.Scan(values...); err != nil {
//             log.Fatal("Error scanning row:", err)
//         }
//
//         // Print column names and their values
//         for i, colName := range columns {
//             fmt.Printf("%s: %v\n", colName, *(values[i].(*interface{})))
//             // fmt.Printf("%s: %v\n", colName, values[i])
//         }
//     }
//
//     fmt.Println("Connected to the database successfully.")
// }
