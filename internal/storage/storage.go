package storage

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

const (
	pathToDB = "merge.db"
)

type LogRecord struct {
	LogLavel  string `json:"log_level"`
	Timestamp string `json:"timestamp"`
	Message   string `json:"message"`
}

//CreateDB creates a temp SQLite database.
func CreateDB() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", pathToDB)
	if err != nil {
		return nil, fmt.Errorf("can't open database: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("can't connect to database: %w", err)
	}
	return db, nil
}

//CreateLogTable creates a table in the database.
func CreateLogTable(db *sql.DB) error {
	q := `CREATE TABLE IF NOT EXISTS logs (
		loglevel TEXT       NOT NULL,
		timestamp TIMESTAMP NOT NULL,
		message   TEXT      NOT NULL
	)`

	_, err := db.Exec(q)
	if err != nil {
		return fmt.Errorf("can't create table: %w", err)
	}
	return nil
}

//RemoveDB removes a temp SQLite database.
func RemoveDB() error {
	err := os.Remove(pathToDB)
	if err != nil {
		return fmt.Errorf("can't remove database: %w", err)
	}
	return nil
}

//GetLogs selects logs from database and writes to file.
func GetLogs(db *sql.DB, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("can't create file: %W", err)
	}
	defer file.Close()

	q := `SELECT * FROM logs ORDER BY timestamp ASC`

	rows, err := db.Query(q)
	if err != nil {
		return fmt.Errorf("query error: %W", err)
	}
	var logLine LogRecord
	for rows.Next() {
		if err := rows.Scan(&logLine.LogLavel, &logLine.Timestamp, &logLine.Message); err != nil {
			return err
		}
		data, err := json.Marshal(&logLine)
		if err != nil {
			return fmt.Errorf("marshal error: %w", err)
		}
		_, err = file.Write(data)
		if err != nil {
			return fmt.Errorf("write error: %w", err)
		}
		_, err = file.WriteString("\n")
		if err != nil {
			return fmt.Errorf("write error: %w", err)
		}
	}
	return nil
}

//TransferLogToDB reads pathToLog line by line and writes to db.
func TransferLogToDB(db *sql.DB, pathToLog string) error {
	file, err := os.Open(pathToLog)
	if err != nil {
		return fmt.Errorf("can't open file: %w", err)
	}
	defer file.Close()

	dec := json.NewDecoder(file)
	q := `INSERT INTO logs VALUES (?, ?, ?)`
	stmt, err := db.Prepare(q)
	if err != nil {
		return err
	}
	var logLine *LogRecord
	for dec.More() {
		err = dec.Decode(logLine)
		if err != nil {
			return fmt.Errorf("decode error: %w", err)
		}
		_, err = stmt.Exec(logLine.LogLavel, logLine.Timestamp, logLine.Message)
		if err != nil {
			return fmt.Errorf("can't save log: %w", err)
		}
	}
	return nil
}
