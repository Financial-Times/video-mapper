package main

import (
	"encoding/json"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"bytes"
	"errors"
	"fmt"
	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/service-status-go/gtg"
	"github.com/Financial-Times/service-status-go/httphandlers"
	"github.com/Financial-Times/message-queue-go-producer/producer"
	"github.com/Financial-Times/message-queue-gonsumer/consumer"
	"github.com/gorilla/mux"
	"github.com/jawher/mow.cli"
	"github.com/satori/go.uuid"
	"html"
	"io/ioutil"
	"strings"
	"time"
	"github.com/gorilla/handlers"
)

const videoContentURIBase = "http://brightcove-video-model-mapper-iw-uk-p.svc.ft.com/video/model/"
const brigthcoveAuthority = "http://api.ft.com/system/BRIGHTCOVE"
const videoMediaTypeBase = "video"
const brightcoveOrigin = "http://cmdb.ft.com/systems/brightcove"
const dateFormat = "2006-01-02T03:04:05.000Z0700"
const ftBrandID = "http://api.ft.com/things/dbb0bdae-1f0c-11e4-b0cb-b2227cce2b54"
const defaultVideoBody = "video"
const publishedDate = "published_at"
const canBeDistributedYes = "yes"
const typeVideo = "Video"

type publicationEvent struct {
	ContentURI   string   `json:"contentUri"`
	Payload      *payload `json:"payload,omitempty"`
	LastModified string   `json:"lastModified"`
}

type identifier struct {
	Authority       string `json:"authority"`
	IdentifierValue string `json:"identifierValue"`
}

type brand struct {
	ID string `json:"id"`
}

type payload struct {
	UUID               string       `json:"uuid"`
	Title              string       `json:"title"`
	Byline             string       `json:"byline,omitempty"`
	Identifiers        []identifier `json:"identifiers"`
	Brands             []brand      `json:"brands"`
	PublishedDate      string       `json:"publishedDate"`
	MediaType          string       `json:"mediaType,omitempty"`
	PublishReference   string       `json:"publishReference"`
	LastModified       string       `json:"lastModified"`
	FirstPublishedDate string       `json:"firstPublishedDate"`
	CanBeDistributed   string       `json:"canBeDistributed"`
	Body               string       `json:"body"`
	Type               string       `json:"type"`
}

type videoMapper struct {
	messageConsumer *consumer.MessageConsumer
	messageProducer *producer.MessageProducer
}

func main() {
	app := cli.App("video-mapper", "Catch native video content transform into Content and send back to queue.")
	appSystemCode := app.String(cli.StringOpt{
		Name:   "app-system-code",
		Value:  "video-mapper",
		Desc:   "The system code of this service",
		EnvVar: "APP_SYSTEM_CODE",
	})
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
		v = videoMapper{messageConsumer: nil, messageProducer: &messageProducer}
		messageConsumer := consumer.NewConsumer(consumerConfig, v.queueConsume, &http.Client{})
		v.messageConsumer = &messageConsumer
		hc := &healthcheck{
			client: http.Client{},
			consumerConf: consumerConfig,
			appSystemCode: *appSystemCode,
		}
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

	r.Path("/__health").Handler(handlers.MethodHandler{"GET": http.HandlerFunc(fthealth.Handler(hc.createHC()))})
	gtgHandler := httphandlers.NewGoodToGoHandler(gtg.StatusChecker(hc.gtgCheck))
	r.Path("/__gtg").Handler(handlers.MethodHandler{"GET": http.HandlerFunc(gtgHandler)})

	http.Handle("/", r)
	port := 8080 //hardcoded for now
	infoLogger.Printf("Starting to listen on port [%d]", port)
	err := http.ListenAndServe(":"+strconv.Itoa(port), nil)
	if err != nil {
		errorLogger.Panicf("Couldn't set up HTTP listener: %+v\n", err)
	}
}

func (v videoMapper) consumeUntilSigterm() {
	infoLogger.Printf("Starting queue consumer: %#v", v.messageConsumer)
	var consumerWaitGroup sync.WaitGroup
	consumerWaitGroup.Add(1)
	go func() {
		(*v.messageConsumer).Start()
		consumerWaitGroup.Done()
	}()
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	(*v.messageConsumer).Stop()
	consumerWaitGroup.Wait()
}

func (v videoMapper) mapHandler(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		writerBadRequest(w, err)
	}
	tid := r.Header.Get("X-Request-Id")
	m := consumer.Message{
		Body:    string(body),
		Headers: createHeader(tid),
	}
	mappedVideoBytes, _, err := v.transformMsg(m)
	if err != nil {
		writerBadRequest(w, err)
	}
	_, err = w.Write(mappedVideoBytes)
	if err != nil {
		warnLogger.Printf("%v - Writing response error: [%v]", tid, err)
	}
}

func writerBadRequest(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusBadRequest)
	_, err2 := w.Write([]byte(err.Error()))
	if err2 != nil {
		warnLogger.Printf("Couldn't write Bad Request response. %v", err2)
	}
	return
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
	marshalledEvent, contentUUID, err := v.transformMsg(m)
	if err != nil {
		warnLogger.Printf("%v - Error error consuming message: %v", tid, err)
		return
	}
	headers := createHeader(tid)
	err = (*v.messageProducer).SendMessage("", producer.Message{Headers: headers, Body: string(marshalledEvent)})
	if err != nil {
		warnLogger.Printf("%v - Error sending transformed message to queue: %v", tid, err)
	}
	infoLogger.Printf("%v - Mapped and sent for uuid: %v", tid, contentUUID)
}

func (v videoMapper) mapMessage(m consumer.Message) ([]byte, string, error) {
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
		return nil, uuid, err
	}

	publishEvent, err := isPublishEvent(brightcoveVideo)
	if err != nil {
		return nil, uuid, err
	}
	//it's an unpublish event
	if !publishEvent {
		marshalledPubEvent, err = buildAndMarshalPublicationEvent(nil, contentURI, lastModified, publishReference)
		return marshalledPubEvent, uuid, err
	}
	publishedDate, _ := getPublishedDate(brightcoveVideo) // at this point we know there is no error
	mediaType := getMediaType(brightcoveVideo, publishReference)
	title, err := get("name", brightcoveVideo)
	if err != nil {
		warnLogger.Printf("%v - name field of native brightcove video JSON is null, title would be empty, can't allow that, skipping %v .", publishReference, uuid)
		return nil, uuid, err
	}
	byline := getByline(brightcoveVideo, publishReference)
	i := identifier{
		Authority:       brigthcoveAuthority,
		IdentifierValue: id,
	}
	b := brand{
		ID: ftBrandID,
	}
	body := getBody(brightcoveVideo)

	firstPublishedDate := getFirstPublishedDate(brightcoveVideo)

	p := &payload{
		UUID:               uuid,
		Title:              title,
		Byline:             byline,
		Identifiers:        []identifier{i},
		Brands:             []brand{b},
		PublishedDate:      publishedDate,
		MediaType:          mediaType,
		PublishReference:   publishReference,
		LastModified:       lastModified,
		FirstPublishedDate: firstPublishedDate,
		CanBeDistributed:   canBeDistributedYes,
		Body:               body,
		Type:               typeVideo,
	}
	marshalledPubEvent, err = buildAndMarshalPublicationEvent(p, contentURI, lastModified, publishReference)
	return marshalledPubEvent, uuid, err
}

func buildAndMarshalPublicationEvent(p *payload, contentURI, lastModified, pubRef string) (_ []byte, err error) {
	e := publicationEvent{
		ContentURI:   contentURI,
		Payload:      p,
		LastModified: lastModified,
	}
	marshalledEvent, err := unsafeJSONMarshal(e)
	if err != nil {
		warnLogger.Printf("%v - Couldn't marshall event %v, skipping message.", pubRef, e)
		return nil, err
	}
	return marshalledEvent, nil
}

func unsafeJSONMarshal(v interface{}) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	b = bytes.Replace(b, []byte("\\u003c"), []byte("<"), -1)
	b = bytes.Replace(b, []byte("\\u003e"), []byte(">"), -1)
	return b, nil
}

func isPublishEvent(video map[string]interface{}) (bool, error) {
	_, err := getPublishedDate(video)
	if err == nil {
		return true, nil
	}
	if _, present := video["error_code"]; present {
		//it's an unpublish event
		return false, nil
	}
	return false, fmt.Errorf("Could not detect event type for video: [%#v]", video)
}

func buildMediaType(fileName string) (string, error) {
	dot := strings.LastIndex(fileName, ".")
	if dot == -1 {
		return "", fmt.Errorf("extension is missing: [%s]", fileName)
	}
	extension := fileName[dot+1:]
	if extension == "" {
		return "", fmt.Errorf("extension is missing: [%s]", fileName)
	}
	return videoMediaTypeBase + "/" + extension, nil
}

func createHeader(tid string) map[string]string {
	return map[string]string{
		"X-Request-Id":      tid,
		"Message-Timestamp": time.Now().Format(dateFormat),
		"Message-Id":        uuid.NewV4().String(),
		"Message-Type":      "cms-content-published",
		"Content-Type":      "application/json",
		"Origin-System-Id":  "http://cmdb.ft.com/systems/brightcove",
	}
}

func getPublishedDate(video map[string]interface{}) (string, error) {
	publishedAt, err1 := get(publishedDate, video)
	if err1 == nil {
		return publishedAt, nil
	}
	updatedAt, err2 := get("updated_at", video)
	if err2 == nil {
		return updatedAt, nil
	}
	return "", fmt.Errorf("No valid value could be found for publishedDate: [%v] [%v]", err1, err2)
}

func getBody(video map[string]interface{}) string {
	longDescription, _ := get("long_description", video)
	description, _ := get("description", video)
	decidedBody := defaultVideoBody
	if description != "" {
		decidedBody = description
	}
	if longDescription != "" {
		decidedBody = longDescription
	}
	return "<body>" + html.EscapeString(decidedBody) + "</body>"
}

func getMediaType(brightcoveVideo map[string]interface{}, publishReference string) (mediaType string) {
	mediaType = ""
	fileName, err := get("original_filename", brightcoveVideo)
	if err != nil {
		warnLogger.Printf("%v - original_filename field of native brightcove video JSON is null, mediaType will be null.", publishReference)
	} else {
		mediaType, err = buildMediaType(fileName)
		if err != nil {
			warnLogger.Printf("%v - building mediaType error: [%v], mediaType will be null.", publishReference, err)
		}
	}
	return mediaType
}

func getByline(brightcoveVideo map[string]interface{}, publishReference string) string {
	byline := ""
	customFields, ok := brightcoveVideo["custom_fields"]
	if !ok {
		infoLogger.Printf("%v - custom_fields field of native brightcove video JSON is null, byline will be empty.", publishReference)
		return ""
	}
	customFieldsMap, okMap := customFields.(map[string]interface{})
	if !okMap {
		infoLogger.Printf("%v - custom_fields field of native brightcove video JSON is not in valid json format, byline will be empty.", publishReference)
		return ""
	}
	var err error
	byline, err = get("byline", customFieldsMap)
	if err != nil {
		infoLogger.Printf("%v - custom_fields.byline field of native brightcove video JSON is null, byline will be null.", publishReference)
	}
	return byline
}

func getFirstPublishedDate(video map[string]interface{}) string {
	result, err := get(publishedDate, video)
	if err != nil {
		warnLogger.Printf("No valid value could be found for firstPublishedDate: [%v]", err)
	}

	return result
}

func get(key string, brightcoveVideo map[string]interface{}) (val string, _ error) {
	valueI, ok := brightcoveVideo[key]
	if !ok {
		return "", fmt.Errorf("[%s] field of native brightcove video JSON is null", key)
	}
	val, ok = valueI.(string)
	if !ok {
		return "", fmt.Errorf("[%s] field of native brightcove video JSON is not a string", key)
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
