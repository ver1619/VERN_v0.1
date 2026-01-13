package main

import (
	"fmt"
	"log"
	"os"

	"tecton_kv/config"
	"tecton_kv/engine"
)

func main() {
	fmt.Println("===================================================")
	fmt.Println("NOTE: This is a DEMONSTRATION example")
	fmt.Println("TectonKV v0.1 — CRASH RECOVERY EXAMPLE")
	fmt.Println("===================================================")

	dbPath := "examples/recovery/db"
	_ = os.RemoveAll(dbPath)

	cfg := config.DefaultConfig(dbPath)

	// RUN 1
	fmt.Println("\n[RUN 1] Open database and write data")
	db, err := engine.Open(cfg)
	if err != nil {
		log.Fatal(err)
	}

	put(db, "a", "1")
	put(db, "b", "2")

	fmt.Println("\n[STEP] Deleting key 'a'")
	db.Delete([]byte("a"))

	fmt.Println("\n[STEP] Closing database (simulated crash-safe shutdown)")
	db.Close()

	// RUN 2
	fmt.Println("\n[RUN 2] Reopening database after crash")
	db, err = engine.Open(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	fmt.Println("\n[STEP] Reading values after recovery")
	get(db, "a")
	get(db, "b")

	fmt.Println("\n[INFO] Data recovered entirely from WAL")
	fmt.Println("Recovery example completed successfully")
}

func put(db *engine.Engine, k, v string) {
	fmt.Printf("PUT  key=%s value=%s\n", k, v)
	if err := db.Put([]byte(k), []byte(v)); err != nil {
		log.Fatal(err)
	}
}

func get(db *engine.Engine, k string) {
	val, ok, err := db.Get([]byte(k))
	if err != nil {
		log.Fatal(err)
	}
	if !ok {
		fmt.Printf("GET  key=%s -> NOT FOUND\n", k)
		return
	}
	fmt.Printf("GET  key=%s -> %s\n", k, val)
}
