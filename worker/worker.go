package main

import (
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"log"

	"os"
	"os/signal"
	"syscall"
)

func main() {
	topic := "comments"
	worker, err := ConnectConsumer([]string{"localhost:29092"})
	if err != nil {
		panic(err)
	}
	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {

		panic(err)
	}

	log.Println("Consuming started")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	msgCount := 0

	doneCh := make(chan struct{})

	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println("Error ", err)
			case msg := <-consumer.Messages():
				msgCount++
				var m = new(struct{
				Text string `json:"text" form:"text"`
				})
				fmt.Printf("Received messages ::: %d, topic ::: %s, partition ::: %s \n", msgCount, string(msg.Topic), string(msg.Partition))
				err := json.Unmarshal(msg.Value, &m)
				if err != nil {
					fmt.Println("Coudln't unmarshal comment :::",err)
				}
				fmt.Printf("BODY ::: %s",m)
			case <-sigChan:
				fmt.Println("Interrupted ")
				doneCh <- struct{}{}
			}

		}
	}()
	<-doneCh
	fmt.Printf("Proccessed %d messages \n", msgCount)
	err = worker.Close()
	if err != nil {
		panic(err)
	}
}

func ConnectConsumer(brokerURL []string) (sarama.Consumer, error) {

	cfg := sarama.NewConfig()
	cfg.Consumer.Return.Errors = true
	consumer, err := sarama.NewConsumer(brokerURL, cfg)
	if err != nil {
		return nil, err
	}
	return consumer, err
}
