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
	fmt.Println("TectonKV v0.1 — BASIC USAGE EXAMPLE")
	fmt.Println("===================================================")

	dbPath := "examples/basic/db"
	_ = os.RemoveAll(dbPath)

	// STEP 1: Open database
	fmt.Println("\n[STEP 1] Opening database")
	cfg := config.DefaultConfig(dbPath)
	db, err := engine.Open(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	fmt.Println("Database opened successfully")

	// STEP 2: Put values
	fmt.Println("\n[STEP 2] Writing key-value pairs")
	put(db, "name", "tecton")
	put(db, "version", "v0.1")

	// STEP 3: Read values
	fmt.Println("\n[STEP 3] Reading values")
	get(db, "name")
	get(db, "version")

	// STEP 4: Delete a key
	fmt.Println("\n[STEP 4] Deleting key 'version'")
	db.Delete([]byte("version"))
	get(db, "version")

	fmt.Println("\n[INFO] No SSTable is expected here (memtable is not full)")
	fmt.Println("\nExample completed")
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
