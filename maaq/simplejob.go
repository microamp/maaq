package maaq

import (
	"context"
	"math/rand"
	"time"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo"
	log "github.com/sirupsen/logrus"
)

type SimpleTask struct {
	TaskInfo *taskInfo `json:"taskinfo"`
}

func NewSimpleTask() *SimpleTask {
	now := time.Now()
	return &SimpleTask{
		TaskInfo: &taskInfo{
			State:   stateReady,
			After:   now,
			Created: now,
			Updated: now,
		},
	}
}

type simpleJob struct {
	c    *mongo.Client
	ctx  context.Context
	db   string
	coll string
}

func NewSimpleJob(c *mongo.Client, ctx context.Context) *simpleJob {
	return &simpleJob{
		c:    c,
		ctx:  ctx,
		db:   "maaq",
		coll: "simpletask",
	}
}

func (j *simpleJob) do(d *doc) error {
	log.Debugln("Do some work then sleep for a while")
	time.Sleep(time.Duration(rand.Intn(1e3)) * time.Millisecond * 5)
	return nil
}

// ready -> started
func (j *simpleJob) started() (*doc, error) {
	stateFrom, stateTo := stateReady, stateStarted
	logger := log.WithFields(log.Fields{"from": stateFrom, "to": stateTo})

	now := time.Now()
	find := bson.NewDocument(
		bson.EC.String("taskinfo.state", stateFrom),
		bson.EC.SubDocumentFromElements("taskinfo.after", bson.EC.Time("$lt", now)),
	)
	modify := bson.NewDocument(
		bson.EC.SubDocumentFromElements(
			"$set",
			bson.EC.String("taskinfo.state", stateTo),
			bson.EC.Time("taskinfo.updated", now),
		),
	)
	d, err := findAndModify(j.c, j.ctx, j.db, j.coll, find, modify)
	if err != nil {
		if err.Error() == "mongo: no documents in result" {
			logger.Infoln("No task to fetch. Retrying later...")
			time.Sleep(time.Duration(5) * time.Second)
		} else {
			logger.Errorf("Error updating task state: %s", err)
		}
		return nil, err
	}
	logger.Infoln("Task updated")
	return &d, nil
}

// started -> failed
func (j *simpleJob) failed() (*doc, error) {
	stateFrom, stateTo := stateStarted, stateFailed
	logger := log.WithFields(log.Fields{"from": stateFrom, "to": stateTo})

	now := time.Now()
	find := bson.NewDocument(
		bson.EC.String("taskinfo.state", stateFrom),
	)
	modify := bson.NewDocument(
		bson.EC.SubDocumentFromElements(
			"$set",
			bson.EC.String("taskinfo.state", stateTo),
			bson.EC.Time("taskinfo.updated", now),
		),
	)
	d, err := findAndModify(j.c, j.ctx, j.db, j.coll, find, modify)
	if err != nil {
		logger.Errorf("Error updating task state: %s", err)
		return nil, err
	}
	logger.Infoln("Task updated")
	return &d, nil
}

// started -> successful
func (j *simpleJob) successful() (*doc, error) {
	stateFrom, stateTo := stateStarted, stateSuccessful
	logger := log.WithFields(log.Fields{"from": stateFrom, "to": stateTo})

	now := time.Now()
	find := bson.NewDocument(
		bson.EC.String("taskinfo.state", stateFrom),
	)
	modify := bson.NewDocument(
		bson.EC.SubDocumentFromElements(
			"$set",
			bson.EC.String("taskinfo.state", stateTo),
			bson.EC.Time("taskinfo.updated", now),
		),
	)
	d, err := findAndModify(j.c, j.ctx, j.db, j.coll, find, modify)
	if err != nil {
		logger.Errorf("Error updating task state: %s", err)
		return nil, err
	}
	logger.Infoln("Task updated")
	return &d, nil
}
