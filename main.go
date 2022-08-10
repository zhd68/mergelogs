package main

import (
	"flag"
	"fmt"
	"log"
	"path/filepath"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/zhd68/mergelogs/internal/storage"
)

const (
	LogFilename = "mergedlog.jsonl"
)

var outputDir = flag.String("o", "./", "path to dir with merged logs")

func main() {
	timeStart := time.Now()
	flag.Parse()

	pathMergedLog, err := filepath.Abs(fmt.Sprintf("%s/%s", *outputDir, LogFilename))
	if err != nil {
		log.Fatalf("incorrect path: %v", err)
	}
	pathToLogs := flag.Args()
	if len(pathToLogs) == 0 {
		log.Fatal("log path not specified")
	}
	for idx, path := range pathToLogs {
		pathToLogs[idx], err = filepath.Abs(path)
		if err != nil {
			log.Fatalf("incorrect path: %v", err)
		}
	}
	mergeLogs(pathMergedLog, pathToLogs)
	fmt.Printf("finished in %v sec\n", time.Since(timeStart).Seconds())
}

func mergeLogs(mergeLog string, logs []string) {
	db, err := storage.CreateDB()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	defer storage.RemoveDB()
	err = storage.CreateLogTable(db)
	if err != nil {
		db.Close()
		storage.RemoveDB()
		log.Fatal(err)
	}
	if err != nil {
		log.Fatal(err)
	}
	for _, logFile := range logs {
		timeStart := time.Now()
		fmt.Println("transfer log:", logFile)
		err := storage.TransferLogToDB(db, logFile)
		if err != nil {
			db.Close()
			storage.RemoveDB()
			log.Fatal(err)
		}
		fmt.Printf("transfered in %v sec\n", time.Since(timeStart).Seconds())
	}
	fmt.Println("merge logs:", mergeLog)
	timeStart := time.Now()
	err = storage.GetLogs(db, mergeLog)
	if err != nil {
		db.Close()
		storage.RemoveDB()
		log.Fatal(err)
	}
	fmt.Printf("merged in %v sec\n", time.Since(timeStart).Seconds())
}
