package monza_destination_elasticsearch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/chinmayrelkar/monza"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

func DefaultElasticsearchConfig() Config {
	return Config{
		Config: elasticsearch.Config{
			Addresses:     []string{"http://localhost:9200"},
			RetryOnStatus: []int{502, 503, 504, 429},
			RetryBackoff:  func(i int) time.Duration { return time.Duration(i) * 100 * time.Millisecond },
			MaxRetries:    5,
		},
		NumWorkers:    4,
		FlushInterval: time.Second,
		DefaultIndex:  "monza",
	}
}

type Config struct {
	elasticsearch.Config
	NumWorkers    int
	FlushInterval time.Duration
	DefaultIndex  string
}

func Get(ctx context.Context, config Config) monza.Destination {
	return &client{
		config:           config,
		teardownComplete: make(chan interface{}),
	}
}

type client struct {
	config           Config
	client           *elasticsearch.Client
	eventChannel     chan *monza.Event
	teardownComplete chan interface{}
}

// Setup implements monza.Destination.
func (c *client) Setup(ctx context.Context) error {
	client, err := elasticsearch.NewClient(c.config.Config)
	if err != nil {
		return fmt.Errorf("failed to create Elastic search client: %e", err)
	}

	_, err = client.Ping()
	if err != nil {
		return fmt.Errorf("failed to connect to Elastic search client at %v: %e", c.config.Addresses, err)
	}

	c.client = client
	return c.listen(ctx)
}

// Record implements monza.Destination.
func (c *client) Record(ctx context.Context, event monza.Event) {
	c.eventChannel <- &event
}

// Teardown implements destinations.Destination.
func (c *client) Teardown(ctx context.Context) {
	c.eventChannel <- nil
	<-c.teardownComplete
}

func (c *client) listen(ctx context.Context) error {
	indexer, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:        c.client,
		Index:         c.config.DefaultIndex,
		NumWorkers:    c.config.NumWorkers,
		FlushInterval: c.config.FlushInterval,
	})
	if err != nil {
		return err
	}

	if indexer == nil {
		return errors.New("destinations.elasticsearch: Failed to create Indexer")
	}

	eventChannel := make(chan *monza.Event)
	go func() {
		for {
			e := <-eventChannel
			if e == nil {
				err := indexer.Close(ctx)
				if err != nil {
					logrus.Error(err)
				}
				c.teardownComplete <- nil
				break
			}

			if e.ID == 0 {
				random, _ := uuid.NewRandom()
				e.ID = int64(random.ID()) + time.Now().Unix()
			}

			err := indexer.Add(ctx, esutil.BulkIndexerItem{
				Index:      fmt.Sprintf("%s-monza-%s", e.ServiceID, time.Now().Format("2006-01-02")),
				Action:     "index",
				DocumentID: toElasticsearchDocumentID(*e),
				Body:       bytes.NewReader(e.JSON()),
			})
			if err != nil {
				logrus.Error(err)
			}
		}
	}()

	c.eventChannel = eventChannel
	return nil
}

func toElasticsearchDocumentID(e monza.Event) string {
	return fmt.Sprintf("doc-%d", e.ID)
}
