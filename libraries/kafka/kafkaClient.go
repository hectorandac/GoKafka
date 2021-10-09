package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaClient struct {
	sync.Mutex
	Context     int
	Server      string
	Topic       string
	Group       string
	Producer    kafka.Producer
	Consumer    kafka.Consumer
	AdminClient kafka.AdminClient
}

type Fn func(string) string

const (
	Producer int = 0
	Consumer int = 1
)

func (kafkaClient *KafkaClient) Init() {
	kafkaClient.Lock()
	defer kafkaClient.Unlock()
	switch kafkaClient.Context {
	case Producer:
		p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaClient.Server})
		if err != nil {
			panic(err)
		}
		kafkaClient.Producer = *p
	case Consumer:
		c, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers": kafkaClient.Server,
			"group.id":          kafkaClient.Group,
			"auto.offset.reset": "earliest",
		})
		if err != nil {
			panic(err)
		}
		kafkaClient.Consumer = *c
	}

	a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": kafkaClient.Server})
	if err != nil {
		panic(err)
	}
	kafkaClient.AdminClient = *a
}

func (kafkaClient *KafkaClient) Consum(function Fn) {
	kafkaClient.Consumer.SubscribeTopics([]string{kafkaClient.Topic}, nil)

	for {
		msg, err := kafkaClient.Consumer.ReadMessage(-1)
		if err == nil {
			fmt.Printf("Message on: %s\nProcessed Result: %s\n", msg.TopicPartition, function(string(msg.Value[:])))
		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}
}

func (kafkaClient *KafkaClient) Produce(message string) {
	kafkaClient.Producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &kafkaClient.Topic, Partition: -1},
		Value:          []byte(message),
	}, nil)

	kafkaClient.Producer.Flush(5)
}

func (kafkaClient *KafkaClient) CreateTopic(partitions int, replicationFactor int) []kafka.TopicResult {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	r, err := kafkaClient.AdminClient.CreateTopics(ctx, []kafka.TopicSpecification{{Topic: kafkaClient.Topic, NumPartitions: partitions, ReplicationFactor: replicationFactor}})
	if err != nil {
		fmt.Println(err.Error())
	}
	return r
}

func (kafkaClient *KafkaClient) PrintClusterInfo() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Get the ClusterID
	if c, e := kafkaClient.AdminClient.ClusterID(ctx); e != nil {
		fmt.Printf("😢 Error getting ClusterID\n\tError: %v\n", e)
	} else {
		fmt.Printf("✔️ ClusterID: %v\n", c)
	}

	// Get the ControllerID
	if c, e := kafkaClient.AdminClient.ControllerID(ctx); e != nil {
		fmt.Printf("😢 Error getting ControllerID\n\tError: %v\n", e)
	} else {
		fmt.Printf("✔️ ControllerID: %v\n", c)
	}

	// Get some metadata
	if md, e := kafkaClient.AdminClient.GetMetadata(nil, false, int(5*time.Second)); e != nil {
		fmt.Printf("😢 Error getting cluster Metadata\n\tError: %v\n", e)
	} else {
		// Print the originating broker info
		fmt.Printf("✔️ Metadata [Originating broker]\n")
		b := md.OriginatingBroker
		fmt.Printf("\t[ID %d] %v\n", b.ID, b.Host)

		// Print the brokers
		fmt.Printf("✔️ Metadata [brokers]\n")
		for _, b := range md.Brokers {
			fmt.Printf("\t[ID %d] %v:%d\n", b.ID, b.Host, b.Port)
		}
		// Print the topics
		fmt.Printf("✔️ Metadata [topics]\n")
		for _, t := range md.Topics {
			fmt.Printf("\t(%v partitions)\t%v\n", len(t.Partitions), t.Topic)
		}
	}
}
