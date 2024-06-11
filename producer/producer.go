package main

import (
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	fiber "github.com/gofiber/fiber/v2"
	"log"
)

type Comment struct {
	Text string `json:"text" form:"text"`
}

func main() {
app := fiber.New()
fi := app.Group("/api/v1")
fi.Post("/comments",createComment)
app.Listen(":3000")
}

func createComment(ctx *fiber.Ctx) error {
	var cmt *Comment
	err := ctx.BodyParser(cmt)
	if err != nil {
		log.Println("Error while parsing request ",err)
		ctx.Status(400).JSON(&fiber.Map{
			"success":false,
			"message":err,
		})
		return err
	}


	cmtBytes, err := json.Marshal(cmt)

	PushToQueue("comments",cmtBytes)
	err = ctx.Status(200).JSON(&fiber.Map{
		"success":true,
		"message":"Pushed to ququqe",
		"comment":cmt,
	})

	if err != nil {
		log.Println("Error whle pushing to queue, ",err)
		ctx.Status(500).JSON(&fiber.Map{
			"success":false,
			"message":err,

		})

		return err
	}
	return nil
}

func PushToQueue(topic string, msg []byte) error {


	brokerURL := []string{"localhost:29092"}
	producer, err := ConnectProducer(brokerURL)
	if err != nil {
		return err
	}
	defer producer.Close()

	prodMsg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(msg),
	}
	partition, offset, err := producer.SendMessage(prodMsg)
	if err != nil {

		return err
	}
	fmt.Printf("Message sent successfully in topic %s::: partition %d ::: offset %d :::\n",topic,partition,offset)
	return nil
}

func ConnectProducer(url []string) (sarama.SyncProducer,error) {

	cfg:= sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Retry.Max = 5

	producer, err := sarama.NewSyncProducer(url, cfg)
	if err != nil {
		return nil,err
	}

	return producer,nil
}
