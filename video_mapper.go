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
	"time"
	"github.com/satori/go.uuid"
)

const videoContentURIBase = "http://brightcove-video-model-mapper-iw-uk-p.svc.ft.com/video/model/"
const brigthcoveAuthority = "http://api.ft.com/system/BRIGHTCOVE"
const videoMediaTypeBase = "video/"
const brightcoveOrigin = "http://cmdb.ft.com/systems/brightcove"

type publicationEvent struct {
	ContentURI   string  `json:"contentUri"`
	Payload      payload `json:"payload"`
	LastModified string  `json:"lastModified"`
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
			AutoCommitEnable:     true,
			AuthorizationKey:     *authorization,
		}
		producerConfig := producer.MessageProducerConfig{
			Addr:          (*addresses)[0],
			Topic:         *writeTopic,
			Queue:         *writeQueue,
			Authorization: *authorization,
		}
		infoLogger.Println(prettyPrintConfig(consumerConfig, producerConfig))
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
	tid := r.Header.Get("X-Request-Id")
	m := consumer.Message{
		Body: string(body),
		Headers: map[string]string{
			"X-Request-Id":      tid,
			"Message-Timestamp": time.Now().Format("2016-04-29T11:02:58.304Z"),
			"Message-Id":        uuid.NewV4().String(),
			"Message-Type":      "cms-content-published",
			"Content-Type":      "application/json",
			"Origin-System-Id":  "http://cmdb.ft.com/systems/brightcove",
		},
	}
	mappedVideoBytes, _, err := v.transformMsg(m)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}
	_, err = w.Write(mappedVideoBytes)
	if err != nil {
		warnLogger.Printf("%v - Writing response error: [%v]", tid, err)
	}
}

func (v videoMapper) transformMsg(m consumer.Message) (marshalledEvent []byte, uuid string, err error) {
	tid := m.Headers["X-Request-Id"]
	marshalledEvent, uuid, err = v.mapMessage(m)
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
	marshalledEvent, contentUuid, err := v.transformMsg(m)
	if err != nil {
		warnLogger.Printf("%v - Error error consuming message: %v", tid, err)
		return
	}
	headers := map[string]string{
		"X-Request-Id":      tid,
		"Message-Timestamp": time.Now().Format("2016-04-29T11:02:58.304Z"),
		"Message-Id":        uuid.NewV4().String(),
		"Message-Type":      "cms-content-published",
		"Content-Type":      "application/json",
		"Origin-System-Id":  "http://cmdb.ft.com/systems/brightcove",
	}
	err = (*v.messageProducer).SendMessage("", producer.Message{Headers: headers, Body: string(marshalledEvent)})
	if err != nil {
		warnLogger.Printf("%v - Error sending transformed message to queue: %v", tid, err)
	}
	infoLogger.Printf("%v - Mapped and sent for uuid: %v", tid, contentUuid)
}

func (v videoMapper) mapMessage(m consumer.Message) (marshalledPubEvent []byte, uuid string, err error) {
	var brightcoveVideo map[string]interface{}
	if err := json.Unmarshal([]byte(m.Body), &brightcoveVideo); err != nil {
		return nil, "", fmt.Errorf("Video JSON from Brightcove couldn't be unmarshalled. Skipping invalid JSON: %v", m.Body)
	}
	publishReference := m.Headers["X-Request-Id"]
	if publishReference == "" {
		return nil, "", errors.New("X-Request-Id not found in kafka message headers. Skipping message")
	}
	lastModified := m.Headers["Message-Timestamp"]
	if lastModified == "" {
		return nil, "", errors.New("Message-Timestamp not found in kafka message headers. Skipping message")
	}
	return v.mapBrightcoveVideo(brightcoveVideo, publishReference, lastModified)
}

func (v videoMapper) mapBrightcoveVideo(brightcoveVideo map[string]interface{}, publishReference, lastModified string) (marshalledPubEvent []byte, uuid string, err error) {
	uuid, err = get("uuid", brightcoveVideo)
	if err != nil {
		return nil, "", err
	}
	contentURI := videoContentURIBase + uuid

	id, err := get("id", brightcoveVideo)
	if err != nil {
		return nil, "", err
	}

	publishedDate, err := get("updated_at", brightcoveVideo)
	if err != nil {
		return nil, "", err
	}

	mediaType := videoMediaTypeBase
	videoName, err := get("name", brightcoveVideo)
	if err != nil {
		warnLogger.Printf("filename field of native brightcove video JSON is null, type will be video/.")
	} else {
		extension := strings.TrimPrefix(filepath.Ext(videoName), ".")
		mediaType = mediaType + extension
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
	e := publicationEvent{
		ContentURI:   contentURI,
		Payload:      p,
		LastModified: lastModified,
	}
	marshalledEvent, err := json.Marshal(e)
	if err != nil {
		warnLogger.Printf("Couldn't marshall event %v, skipping message.", e)
		return nil, "", err
	}
	return marshalledEvent, uuid, nil
}

func get(key string, brightcoveVideo map[string]interface{}) (val string, err error) {
	valueI, ok := brightcoveVideo[key]
	if !ok {
		return "", fmt.Errorf("%s field of native brightcove video JSON is null. Skipping message", key)
	}
	val, ok = valueI.(string)
	if !ok {
		return "", fmt.Errorf("%s field of native brightcove video JSON is not a string. Skipping message", key)
	}
	return val, nil
}

func prettyPrintConfig(c consumer.QueueConfig, p producer.MessageProducerConfig) string {
	return fmt.Sprintf("Config: [\n\t%s\n\t%s\n]", prettyPrintConsumerConfig(c), prettyPrintProducerConfig(p))
}

func prettyPrintConsumerConfig(c consumer.QueueConfig) string {
	return fmt.Sprintf("consumerConfig: [\n\t\taddr: [%v]\n\t\tgroup: [%v]\n\t\ttopic: [%v]\n\t\treadQueueHeader: [%v]\n\t]", c.Addrs, c.Group, c.Topic, c.Queue)
}

func prettyPrintProducerConfig(p producer.MessageProducerConfig) string {
	return fmt.Sprintf("producerConfig: [\n\t\taddr: [%v]\n\t\ttopic: [%v]\n\t\twriteQueueHeader: [%v]\n\t]", p.Addr, p.Topic, p.Queue)
}
