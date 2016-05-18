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

	"errors"
	"fmt"
	"github.com/Financial-Times/message-queue-go-producer/producer"
	"github.com/Financial-Times/message-queue-gonsumer/consumer"
	"github.com/gorilla/mux"
	"github.com/jawher/mow.cli"
	"io/ioutil"
	"strings"
)

const videoContentUriBase = "http://brightcove-video-model-mapper-iw-uk-p.svc.ft.com/video/model/"
const brigthcoveAuthority = "http://api.ft.com/system/BRIGHTCOVE"
const viodeMediaTypeBase = "video/"
const brightcoveOrigin = "http://cmdb.ft.com/systems/brightcove"

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

type errorString struct {
	s string
}

func (e *errorString) Error() string {
	return e.s
}

func New(text string) error {
	return &errorString{text}
}

func main() {
	app := cli.App("video-mapper", "Catch native video content transform into Content and send back to queue.")
	addresses := app.Strings(cli.StringsOpt{
		Name:   "queue-addresses",
		Value:  nil,
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
		if len(*addresses) == 0 {
			errorLogger.Println("No queue address provided. Quitting...")
			cli.Exit(1)
		}
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
		infoLogger.Printf("[%s]", prettyPrintVideoMapper(consumerConfig, producerConfig))
		messageProducer := producer.NewMessageProducer(producerConfig)
		var v videoMapper
		v = videoMapper{nil, &messageProducer}
		messageConsumer := consumer.NewConsumer(consumerConfig, v.queueConsume, http.Client{})
		v.messageConsumer = &messageConsumer
		hc := &healthcheck{client: http.Client{}, consumerConf: consumerConfig}
		go v.listen(hc)
		v.consumeUntilSigterm()
	}
	err := app.Run(os.Args)
	if err != nil {
		println(err)
	}
}

func prettyPrintVideoMapper(c consumer.QueueConfig, p producer.MessageProducerConfig) string {
	return fmt.Sprintf("videoMapper: [\n\t%s\n\t%s\n]", prettyPrintConsumerConfig(c), prettyPrintProducerConfig(p))
}

func prettyPrintConsumerConfig(c consumer.QueueConfig) string {
	return fmt.Sprintf("consumerConfig: [\n\t\taddr: [%v]\n\t\tgroup: [%v]\n\t\ttopic: [%v]\n\t\treadQueueHeader: [%v]\n\t]", c.Addrs, c.Group, c.Topic, c.Queue)
}

func prettyPrintProducerConfig(p producer.MessageProducerConfig) string {
	return fmt.Sprintf("producerConfig: [\n\t\taddr: [%v]\n\t\ttopic: [%v]\n\t\twriteQueueHeader: [%v]\n\t]", p.Addr, p.Topic, p.Queue)
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
	infoLogger.Printf("Starting queue consumer: %# v", v.messageConsumer)
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
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}
	m := consumer.Message{
		Body: string(body),
		Headers: map[string]string{
			"X-Request-Id":      r.Header.Get("X-Request-Id"),
			"Message-Timestamp": r.Header.Get("X-Message-Timestamp"),
			"Message-Id":        "f03d84da-c400-4165-87dc-9b026fbeaa6d",
			"Message-Type":      "cms-content-published",
			"Content-Type":      "application/json",
			"Origin-System-Id":  "http://cmdb.ft.com/systems/brightcove",
		},
	}
	mappedVideoBytes, _, err := v.httpConsume(m)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}
	w.Write(mappedVideoBytes)
}

func (v videoMapper) httpConsume(m consumer.Message) ([]byte, string, error) {
	tid := m.Headers["X-Request-Id"]
	marshalledEvent, uuid, err := v.mapMessage(m)
	if err != nil {
		warnLogger.Printf("%v - Mapping error: [%v]", tid, err.Error())
		return nil, "", err
	}
	return marshalledEvent, uuid, nil
}

func (v videoMapper) queueConsume(m consumer.Message) {
	tid := m.Headers["X-Request-Id"]
	if m.Headers["Origin-System-Id"] != brightcoveOrigin {
		infoLogger.Printf("%v - Ignoring message with different Origin-System-Id %v", tid, m.Headers["Origin-System-Id"])
		return
	}
	marshalledEvent, uuid, err := v.httpConsume(m)
	if err != nil {
		warnLogger.Printf("%v - Error error consuming message: %v", tid, err)
		return
	}
	err = (*v.messageProducer).SendMessage("", producer.Message{Headers: m.Headers, Body: string(marshalledEvent)})
	if err != nil {
		warnLogger.Printf("%v - Error sending transformed message to queue: %v", tid, err)
	}
	infoLogger.Printf("%v - Mapped and sent for uuid: %v", tid, uuid)
}

func (v videoMapper) mapMessage(m consumer.Message) ([]byte, string, error) {
	var brightcoveVideo map[string]interface{}
	if err := json.Unmarshal([]byte(m.Body), &brightcoveVideo); err != nil {
		return nil, "", errors.New(fmt.Sprintf("Video JSON from Brightcove couldn't be unmarshalled. Skipping invalid JSON: %v", m.Body))
	}
	publishReference := m.Headers["X-Request-Id"]
	if publishReference == "" {
		return nil, "", errors.New("X-Request-Id not found in kafka message headers. Skipping message.")
	}
	lastModified := m.Headers["Message-Timestamp"]
	if lastModified == "" {
		return nil, "", errors.New("Message-Timestamp not found in kafka message headers. Skipping message.")
	}
	return v.mapBrightcoveVideo(brightcoveVideo, publishReference, lastModified)
}

func (v videoMapper) mapBrightcoveVideo(brightcoveVideo map[string]interface{}, publishReference, lastModified string) ([]byte, string, error) {
	var uuidI interface{}
	uuidI, ok := brightcoveVideo["uuid"]
	if !ok {
		return nil, "", errors.New(fmt.Sprintf("uuid field of native brightcove video JSON is null. Skipping message."))
	}
	uuid, ok := uuidI.(string)
	if !ok {
		return nil, "", errors.New(fmt.Sprintf("uuid field of native brightcove video JSON is not a string. Skipping message."))
	}
	contentUri := videoContentUriBase + uuid

	idI, ok := brightcoveVideo["id"]
	if !ok {
		return nil, "", errors.New(fmt.Sprintf("id field of native brightcove video JSON is null. Skipping message."))
	}
	id, ok := idI.(string)
	if !ok {
		return nil, "", errors.New(fmt.Sprintf("id field of native brightcove video JSON is not a string. Skipping message."))
	}

	publishedDateI, ok := brightcoveVideo["updated_at"]
	if !ok {
		return nil, "", errors.New(fmt.Sprintf("updated_at field of native brightcove video JSON is null. Skipping message."))
	}
	publishedDate, ok := publishedDateI.(string)
	if !ok {
		return nil, "", errors.New(fmt.Sprintf("updated_at field of native brightcove video JSON is not a string. Skipping message."))

	}

	mediaType := viodeMediaTypeBase
	fileNameI, ok := brightcoveVideo["name"]
	if !ok {
		warnLogger.Printf("filename field of native brightcove video JSON is null, type will be video/.")
	} else {
		fileName, ok := fileNameI.(string)
		if !ok {
			warnLogger.Printf("filename field of native brightcove video JSON is not as string, type will be video/.")
		} else {
			extension := strings.TrimPrefix(filepath.Ext(fileName), ".")
			mediaType = mediaType + extension
		}
	}

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
		return nil, "", err
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
		return nil, "", err
	}
	return marshalledEvent, uuid, nil
}
