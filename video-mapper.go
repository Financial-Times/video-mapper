package main

import (
	"github.com/Financial-Times/message-queue-gonsumer/consumer"
	"github.com/Financial-Times/message-queue-go-producer/producer"
	"github.com/jawher/mow.cli"
	"os"
	"net/http"
	"github.com/kr/pretty"
	"sync"
	"os/signal"
	"syscall"
)

func main() {
	app := cli.App("video-mapper", "Catch native video content transform into Content and send back to queue.")
	addresses := app.Strings(cli.StringsOpt{
		Name:   "queue-addresses",
		Value:  []string{},
		Desc:   "Addresses to connect to the queue (hostnames).",
		EnvVar: "Q_ADDR",
	})
	group := app.String(cli.StringOpt{
		Name:   "group",
		Value:  "",
		Desc:   "Group used to read the messages from the queue.",
		EnvVar: "Q_GROUP",
	})
	readTopic := app.String(cli.StringOpt{
		Name:   "read-topic",
		Value:  "",
		Desc:   "The topic to read the meassages from.",
		EnvVar: "Q_READ_TOPIC",
	})
	readQueue := app.String(cli.StringOpt{
		Name:   "read-queue",
		Value:  "",
		Desc:   "The queue to read the meassages from.",
		EnvVar: "Q_READ_QUEUE",
	})
	writeTopic := app.String(cli.StringOpt{
		Name:   "write-topic",
		Value:  "",
		Desc:   "The topic to write the meassages to.",
		EnvVar: "Q_WRITE_TOPIC",
	})
	writeQueue := app.String(cli.StringOpt{
		Name:   "write-queue",
		Value:  "",
		Desc:   "The queue to write the meassages to.",
		EnvVar: "Q_WRITE_QUEUE",
	})
	authorization := app.String(cli.StringOpt{
		Name:   "authorization",
		Value:  "",
		Desc:   "Authorization key to access the queue.",
		EnvVar: "Q_AUTHORIZATION",
	})
	app.Action = func() {
		initLogs(os.Stdout, os.Stdout, os.Stderr)
		infoLogger.Println("Hi.")
		consumerConfig := consumer.QueueConfig{
			Addrs: *addresses,
			Group: *group,
			Topic: *readTopic,
			Queue: *readQueue,
			ConcurrentProcessing: false,
			AuthorizationKey: *authorization,
		}
		producerConfig := producer.MessageProducerConfig{
			Addr: (*addresses)[0],
			Topic: *writeTopic,
			Queue: *writeQueue,
			Authorization: *authorization,
		}
		messageProducer := producer.NewMessageProducer(producerConfig)
		headers := make(map[string]string)
		messageProducer.SendMessage("", producer.Message{Headers: headers, Body: ""})
		launchConsumer(consumerConfig)
	}
	err := app.Run(os.Args)
	if err != nil {
		println(err)
	}
}

func launchConsumer(consumerConfig consumer.QueueConfig) {
	messageConsumer := consumer.NewConsumer(consumerConfig, func(m consumer.Message) { infoLogger.Println(m) }, http.Client{})
	infoLogger.Printf("Starting queue consumer: %# v", pretty.Formatter(messageConsumer))
	var consumerWaitGroup sync.WaitGroup
	consumerWaitGroup.Add(1)
	go func() {
		messageConsumer.Start()
		consumerWaitGroup.Done()
	}()
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	messageConsumer.Stop()
	consumerWaitGroup.Wait()
}
