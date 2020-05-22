package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Consumer struct {
	*kafka.Consumer
}

type MessageCallback func(context.Context, *kafka.Message)

type ErrorCallBack func(error, bool)

func NewConsumer(conf *kafka.ConfigMap) (*Consumer, error) {
	c, err := kafka.NewConsumer(conf)
	if err != nil {
		return nil, err
	}
	localC := &Consumer{c}
	return localC, nil
}

func (c *Consumer) ConsumeWithSpan(ctx context.Context, callback MessageCallback, errCallback ErrorCallBack, topic string) {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	err := c.Subscribe(topic, nil)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
	}
	run := true
	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false

		case ev := <-c.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				c.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				fmt.Fprintf(os.Stderr, "%% %v\n", e)
				c.Unassign()
			case *kafka.Message:
				callback(ctx, e)
			case kafka.PartitionEOF:
				errCallback(e.Error, false)
			case kafka.Error:
				errCallback(errors.New(e.Error()), true)
				break
			}
		}
	}
	fmt.Printf("Closing consumer\n")
	c.Close()
}

func (c *Consumer) AsyncConsumeWithSpan(ctx context.Context, callback MessageCallback, errCallback ErrorCallBack, topic string) {
	go c.ConsumeWithSpan(ctx, callback, errCallback, topic)
}
