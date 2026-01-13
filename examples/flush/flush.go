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
	fmt.Println("TectonKV v0.1 — MEMTABLE FLUSH EXAMPLE")
	fmt.Println("===================================================")

	dbPath := "examples/flush/db"
	_ = os.RemoveAll(dbPath)

	// Force very small memtable to trigger flush
	cfg := config.DefaultConfig(dbPath)
	cfg.MemtableSizeBytes = 1

	// STEP 1
	fmt.Println("\n[STEP 1] Opening database (flush threshold = 1 byte)")
	db, err := engine.Open(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// STEP 2
	fmt.Println("\n[STEP 2] Writing keys (this WILL trigger flushes)")
	put(db, "k1", "one")
	put(db, "k2", "two")
	put(db, "k3", "three")

	// STEP 3
	fmt.Println("\n[STEP 3] Reading keys after flush")
	get(db, "k1")
	get(db, "k2")
	get(db, "k3")

	fmt.Println("\n[INFO] SSTables should exist under examples/flush/db/sstables/")
	fmt.Println("Flush example completed successfully")
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
