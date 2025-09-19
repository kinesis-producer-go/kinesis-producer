package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/go-faker/faker/v4"
)

type Data struct {
	GroupID  string `json:"group_id" faker:"oneof: one, two, three"`
	Word     string `json:"word" faker:"word"`
	TimeUnix int64  `json:"time_unix"`
}

func TestExample(t *testing.T) {
	t.Skipf("Skip by default")

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, _ := config.LoadDefaultConfig(context.TODO())
	client := kinesis.NewFromConfig(cfg)
	pr := New(&Config{
		StreamName:          aws.String("test"),
		BacklogCount:        2000,
		Client:              client,
		Logger:              logger,
		Verbose:             true,
		AggregateBatchCount: 10,              // Aggregate max 10 records per batch
		AggregateBatchSize:  1024,            // Aggregate when reaching 1KB
		FlushInterval:       1 * time.Second, // Flush every 1 second for testing
	})

	pr.Start()

	// Handle failures
	go func() {
		for r := range pr.NotifyFailures() {
			// r contains `Data`, `PartitionKey` and `Error()`
			logger.Error("detected put failure", "error", r.error)
		}
	}()

	go func() {
		for i := 0; i < 5000; i++ {
			data := &Data{}
			err := faker.FakeData(&data)
			if err != nil {
				fmt.Println(data)
			}
			data.TimeUnix = time.Now().Unix()
			jsonBytes, err := json.Marshal(data)
			if err != nil {
				panic(err)
			}
			err = pr.Put(jsonBytes)
			fmt.Printf("%s\n", jsonBytes)
			if err != nil {
				logger.Error("error producing", "error", err)
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()

	time.Sleep(1 * time.Minute)
	pr.Stop()
}
