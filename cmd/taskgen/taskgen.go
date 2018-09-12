package main

import (
	"context"

	"github.com/microamp/maaq/maaq"

	"github.com/mongodb/mongo-go-driver/mongo"
	log "github.com/sirupsen/logrus"
)

func main() {
	ctx := context.Background()

	c, err := mongo.NewClient("mongodb://127.0.0.1:27017")
	if err != nil {
		log.Fatalf("Error setting up MongoDB client: %s", err)
	}
	err = c.Connect(ctx)
	if err != nil {
		log.Fatalf("Error connecting to MongoDB: %s", err)
	}

	task := maaq.NewSimpleTask()
	inserted, err := c.Database("maaq").Collection("simpletask").InsertOne(ctx, task)
	if err != nil {
		log.Fatalf("Error writing task to DB: %s", err)
	}
	log.Infof("Inserted task: %s", inserted.InsertedID)
}
