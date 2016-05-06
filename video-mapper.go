package main

import (
	"encoding/json"
	"github.com/Financial-Times/message-queue-go-producer/producer"
	"github.com/Financial-Times/message-queue-gonsumer/consumer"
	"github.com/jawher/mow.cli"
	"github.com/kr/pretty"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
)

const videoContentUriBase = "http://video-mapper-iw-uk-p.svc.ft.com/video/model/"
const brigthcoveAuthority = "http://api.ft.com/system/BRIGHTCOVE"
const viodeMediaTypeBase = "video/"

type publicationEvent struct {
	contentUri   string `json:"contentUri"`
	payload      string `json:"payload"`
	lastModified string `json:"lastModified"`
}

type identifier struct {
	authority       string `json:"authority"`
	identifierValue string `json:"identifierValue"`
}

type payload struct {
	uuid             string       `json:"uuid"`
	identifiers      []identifier `json:"identifiers"`
	publishedDate    string       `json:"publishedDate"`
	mediaType        string       `json:"mediaType"`
	publishReference string       `json:"publishReference"`
	lastModified     string       `json:"lastModified"`
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
			Addrs:                *addresses,
			Group:                *group,
			Topic:                *readTopic,
			Queue:                *readQueue,
			ConcurrentProcessing: false,
			AuthorizationKey:     *authorization,
		}
		producerConfig := producer.MessageProducerConfig{
			Addr:          (*addresses)[0],
			Topic:         *writeTopic,
			Queue:         *writeQueue,
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
	uuid := brightcoveVideo["uuid"].(string)
	contentUri := videoContentUriBase + uuid
	if uuid == "" {
		contentUri = ""
		warnLogger.Printf("uuid field of native brightcove video JSON is null. uuid and contentUri will be null.")
	}
	id := brightcoveVideo["id"].(string)
	if id == "" {
		warnLogger.Printf("id field of native brightcove video JSON is null. identifier will be null.")
	}
	publishedDate := brightcoveVideo["updated_at"].(string)
	if publishedDate == "" {
		warnLogger.Printf("updated_at field of native brightcove video JSON is null, publisedDate will be null.")
	}
	extension := filepath.Ext(brightcoveVideo["name"].(string))
	mediaType := viodeMediaTypeBase + extension
	publishReference := m.Headers["X-Request-Id"]
	if publishReference == "" {
		warnLogger.Printf("X-Request-Id not found in kafka message headers. publishReference will be null.")
	}
	lastModified := m.Headers["Message-Timestamp"]
	if lastModified == "" {
		warnLogger.Printf("Message-Timestamp not found in kafka message headers. lastModified will be null.")
	}

	i := identifier{
		authority:       brigthcoveAuthority,
		identifierValue: id,
	}
	p := payload{
		uuid:             uuid,
		identifiers:      []identifier{i},
		publishedDate:    publishedDate,
		mediaType:        mediaType,
		publishReference: publishReference,
		lastModified:     lastModified,
	}
	marshalledPayload, err := json.Marshal(p)
	if err != nil {
		warnLogger.Printf("Couldn't marshall payload %v, skipping message.", p)
		return
	}
	e := publicationEvent{
		contentUri:   contentUri,
		payload:      string(marshalledPayload),
		lastModified: lastModified,
	}
	marshalledEvent, err := json.Marshal(e)
	if err != nil {
		warnLogger.Printf("Couldn't marshall event %v, skipping message.", e)
		return
	}
	//		payload: "http://video.ft.com/" + brightcoveVideo["id"].(string),

	//		lastModified: []string{"http://www.ft.com/ontology/content/Video"},
	//ss := "{\"name\":\"Huni\"}"
	//fmt.Println(ss)
	//fmt.Println(strconv.Quote(ss))

	//(*v.messageProducer).SendMessage(id, producer.Message{Headers: m.Headers, Body: string(cocoVideoS)})
	infoLogger.Printf("sending %v", marshalledEvent)
}
