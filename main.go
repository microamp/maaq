package main

import (
	"context"
	"os"
	"os/signal"
	"sync"

	"github.com/microamp/maaq/maaq"
	"github.com/mongodb/mongo-go-driver/mongo"
	log "github.com/sirupsen/logrus"
)

const workers = 10

var ctx = context.Background()

func main() {
	c, err := mongo.NewClient("mongodb://127.0.0.1:27017")
	if err != nil {
		log.Fatalf("Error setting up MongoDB client: %s", err)
	}
	err = c.Connect(ctx)
	if err != nil {
		log.Fatalf("Error connecting to MongoDB: %s", err)
	}
	j := maaq.NewSimpleJob(c, ctx)

	var wg sync.WaitGroup

	done := make(chan bool)
	ch := maaq.Produce(j, done)
	for i := 0; i < workers; i++ {
		go maaq.Consume(j, ch, &wg)
		wg.Add(1)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt)

	go func() {
		<-sigs
		log.Warnln("Interrupt signal detected!")
		close(done)
	}()

	log.Infoln("Waiting for all goroutines to exit gracefully...")
	wg.Wait()
	log.Infoln("All goroutines terminated. Exiting...")
}
