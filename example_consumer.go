package main

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/tidwall/gjson"
)

func main()  {
	group:= "vrk"
	broker := "localhost:9092"
	topic := "data-product"
	kafkaCon, _ := NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               broker,
		"group.id":                        group,
		"session.timeout.ms":              6000,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		// Enable generation of PartitionEOF when the
		// end of a partition is reached.
		"enable.partition.eof": false,
		"auto.offset.reset":    "earliest"})

	kafkaCon.ConsumeWithSpan(context.Background(), callback, errback, topic)
}

func callback(ctx context.Context, data *kafka.Message)  {
	result := gjson.ParseBytes(data.Value).Value().(interface{})
	fmt.Sprint(result)
}

func errback(err error,true bool)  {
	fmt.Sprint(err)
}