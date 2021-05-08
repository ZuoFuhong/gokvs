package main

import (
	"gokvs"
	"gokvs/engines"
	"log"
	"os"
)

const (
	VERSION = "0.1.0"
	ENGINE  = "kvs"
	ADDRESS = "127.0.0.1:9999"
)

func main() {
	path, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	engine, err := engines.NewKvsStore(path)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("kvs-server %s\n", VERSION)
	log.Printf("Storage engine: %s\n", ENGINE)
	log.Printf("Listening on %s\n", ADDRESS)
	server := gokvs.NewKvsServer(engine)
	err = server.Run(ADDRESS)
	if err != nil {
		log.Fatal(err)
	}
}
