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
	"encoding/json"
)

type content struct {
	id string `json:"id"`
	webUrl string `json:"webUrl"`
	types []string `json:"types"`
}

type videoMapper struct {
	messageConsumer *consumer.Consumer
	messageProducer *producer.MessageProducer
}

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
		var v videoMapper
		messageConsumer := consumer.NewConsumer(consumerConfig, v.mapping, http.Client{})
		v = videoMapper{&messageConsumer, &messageProducer}
		hc := &healthcheck{client: http.Client{}, consumerConf: consumerConfig}
		http.HandleFunc("/__health", hc.healthcheck())
		http.HandleFunc("/__gtg", hc.gtg)
		go func() {
			err := http.ListenAndServe(":8080", nil)
			errorLogger.Println(err)
		}()
		v.consumeUntilSigterm()
	}
	err := app.Run(os.Args)
	if err != nil {
		println(err)
	}
}

func (v videoMapper) consumeUntilSigterm() {
	infoLogger.Printf("Starting queue consumer: %# v", pretty.Formatter(v.messageConsumer))
	var consumerWaitGroup sync.WaitGroup
	consumerWaitGroup.Add(1)
	go func() {
		v.messageConsumer.Start()
		consumerWaitGroup.Done()
	}()
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	v.messageConsumer.Stop()
	consumerWaitGroup.Wait()
}

func (v videoMapper) mapping(m consumer.Message) {
	var brightcoveVideo map[string]interface{}
	if err := json.Unmarshal([]byte(m.Body), &brightcoveVideo); err != nil {
		infoLogger.Printf("Video JSON from Brightcove couldn't be unmarshalled. Ignoring not valid JSON: %v", m.Body)
		return
	}

	id := brightcoveVideo["uuid"].(string)
	cocoVideo := content{
		id: id,
		webUrl: "http://video.ft.com/" + brightcoveVideo["id"].(string),
		types: []string{"http://www.ft.com/ontology/content/Video"},
	}

	cocoVideoS, err := json.Marshal(cocoVideo)
	if (err != nil) {
		infoLogger.Printf("Video couldn't be marshalled from struct to JSON string. Ignoring: %v", cocoVideo)
	}

	(*v.messageProducer).SendMessage(id, producer.Message{Headers: m.Headers, Body: string(cocoVideoS)})
	infoLogger.Printf("sending %v", m)
}
