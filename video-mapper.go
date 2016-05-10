package main

import (
	"encoding/json"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"

	"github.com/Financial-Times/message-queue-go-producer/producer"
	"github.com/Financial-Times/message-queue-gonsumer/consumer"
	"github.com/gorilla/mux"
	"github.com/jawher/mow.cli"
	"github.com/kr/pretty"
	"strings"
)

const videoContentUriBase = "http://video-mapper-iw-uk-p.svc.ft.com/video/model/"
const brigthcoveAuthority = "http://api.ft.com/system/BRIGHTCOVE"
const viodeMediaTypeBase = "video/"

type publicationEvent struct {
	ContentUri   string `json:"contentUri"`
	Payload      string `json:"payload"`
	LastModified string `json:"lastModified"`
}

type identifier struct {
	Authority       string `json:"authority"`
	IdentifierValue string `json:"identifierValue"`
}

type payload struct {
	UUID             string       `json:"uuid"`
	Identifiers      []identifier `json:"identifiers"`
	PublishedDate    string       `json:"publishedDate"`
	MediaType        string       `json:"mediaType"`
	PublishReference string       `json:"publishReference"`
	LastModified     string       `json:"lastModified"`
}

type videoMapper struct {
	messageConsumer *consumer.Consumer
	messageProducer *producer.MessageProducer
}

func main() {
	app := cli.App("video-mapper", "Catch native video content transform into Content and send back to queue.")
	addresses := app.Strings(cli.StringsOpt{
		Name:   "queue-addresses",
		Value:  []string{"http://localhost:9090"},
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
		messageConsumer := consumer.NewConsumer(consumerConfig, v.consume, http.Client{})
		v = videoMapper{&messageConsumer, &messageProducer}
		hc := &healthcheck{client: http.Client{}, consumerConf: consumerConfig}
		go v.listen(hc)
		v.consumeUntilSigterm()
	}
	err := app.Run(os.Args)
	if err != nil {
		println(err)
	}
}

func (v videoMapper) listen(hc *healthcheck) {
	r := mux.NewRouter()
	r.HandleFunc("/map", v.mapHandler).Methods("POST")
	r.HandleFunc("/__health", hc.healthcheck()).Methods("GET")
	r.HandleFunc("/__gtg", hc.gtg).Methods("GET")

	http.Handle("/", r)
	port := 8080 //hardcoded for now
	infoLogger.Printf("Starting to listen on port [%d]", port)
	err := http.ListenAndServe(":"+strconv.Itoa(port), nil)
	if err != nil {
		errorLogger.Panicf("Couldn't set up HTTP listener: %+v\n", err)
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

func (v videoMapper) mapHandler(w http.ResponseWriter, r *http.Request) {
	var brightcoveVideo map[string]interface{}

	err := json.NewDecoder(r.Body).Decode(&brightcoveVideo)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	mappedVideoBytes, err := v.mapBrightcoveVideo(brightcoveVideo, r.Header.Get("X-Request-Id"), r.Header.Get("Message-Timestamp"))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.Write(mappedVideoBytes)
}

func (v videoMapper) consume(m consumer.Message) {
	marshalledEvent, err := v.mapMessage(m)
	if err != nil {
		warnLogger.Printf("Mapping error: [%v]", err)
		return
	}
	infoLogger.Printf("sending %v", marshalledEvent)
	//(*v.messageProducer).SendMessage(id, producer.Message{Headers: m.Headers, Body: string(cocoVideoS)})
}

func (v videoMapper) mapMessage(m consumer.Message) ([]byte, error) {
	var brightcoveVideo map[string]interface{}
	if err := json.Unmarshal([]byte(m.Body), &brightcoveVideo); err != nil {
		infoLogger.Printf("Video JSON from Brightcove couldn't be unmarshalled. Ignoring not valid JSON: %v", m.Body)
		return nil, err
	}
	publishReference := m.Headers["X-Request-Id"]
	if publishReference == "" {
		warnLogger.Printf("X-Request-Id not found in kafka message headers. publishReference will be null.")
	}
	lastModified := m.Headers["Message-Timestamp"]
	if lastModified == "" {
		warnLogger.Printf("Message-Timestamp not found in kafka message headers. lastModified will be null.")
	}
	return v.mapBrightcoveVideo(brightcoveVideo, publishReference, lastModified)
}

func (v videoMapper) mapBrightcoveVideo(brightcoveVideo map[string]interface{}, publishReference, lastModified string) ([]byte, error) {
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
	fileName := brightcoveVideo["name"].(string)
	if fileName == "" {
		warnLogger.Printf("filename field of native brightcove video JSON is null, type will be video/.")
	}
	extension := strings.TrimPrefix(filepath.Ext(fileName), ".")
	mediaType := viodeMediaTypeBase + extension
	i := identifier{
		Authority:       brigthcoveAuthority,
		IdentifierValue: id,
	}
	p := payload{
		UUID:             uuid,
		Identifiers:      []identifier{i},
		PublishedDate:    publishedDate,
		MediaType:        mediaType,
		PublishReference: publishReference,
		LastModified:     lastModified,
	}
	marshalledPayload, err := json.Marshal(p)
	if err != nil {
		warnLogger.Printf("Couldn't marshall payload %v, skipping message.", p)
		return nil, err
	}
	//fmt.Println(strconv.Quote(ss))
	e := publicationEvent{
		ContentUri:   contentUri,
		Payload:      string(marshalledPayload),
		LastModified: lastModified,
	}
	marshalledEvent, err := json.Marshal(e)
	if err != nil {
		warnLogger.Printf("Couldn't marshall event %v, skipping message.", e)
		return nil, err
	}
	return marshalledEvent, nil
}
