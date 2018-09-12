package maaq

import (
	"context"
	"time"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo"
)

const (
	stateReady      = "ready"
	stateStarted    = "started"
	stateFailed     = "failed"
	stateSuccessful = "successful"
)

type taskInfo struct {
	State       string    `json:"state"`
	After       time.Time `json:"after"`
	Created     time.Time `json:"created"`
	Updated     time.Time `json:"updated"`
	ErrorReason string    `json:"errorReason"`
}

func findAndModify(c *mongo.Client, ctx context.Context, db, coll string, find, modify *bson.Document) (doc, error) {
	d := doc{}
	result := c.Database(db).Collection(coll).FindOneAndUpdate(ctx, find, modify)
	if err := result.Decode(d); err != nil {
		return nil, err
	}
	return d, nil
}
