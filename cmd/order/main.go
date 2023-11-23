package main

import (
	"database/sql"
	"encoding/json"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rcjeferson/go-intensive/internal/infra/database"
	"github.com/rcjeferson/go-intensive/internal/usecase"
	"github.com/rcjeferson/go-intensive/pkg/rabbitmq"
)

func main() {
	db, err := sql.Open("sqlite3", "db.sqlite3")
	if err != nil {
		panic(err)
	}

	defer db.Close()

	orderRepository := database.NewOrderRepository(db)
	uc := usecase.NewCalculateFinalPrice(orderRepository)

	ch, err := rabbitmq.OpenChannel()
	if err != nil {
		panic(err)
	}

	defer ch.Close()

	msgRabbitMQChannel := make(chan amqp.Delivery)
	go rabbitmq.Consume(ch, msgRabbitMQChannel)
	rabbitmqWorker(msgRabbitMQChannel, uc)
}

func rabbitmqWorker(msgChan chan amqp.Delivery, uc *usecase.CalculateFinalPrice) {
	fmt.Println("Starting RabbitMQ...")

	for msg := range msgChan {
		var input usecase.OrderInput

		err := json.Unmarshal(msg.Body, &input)
		if err != nil {
			panic(err)
		}

		output, err := uc.Execute(input)
		if err != nil {
			panic(err)
		}

		msg.Ack(false)
		fmt.Println("The message has been executed and saved on database:", output)
	}
}
