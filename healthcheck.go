package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/Financial-Times/go-fthealth"
	"github.com/Financial-Times/message-queue-gonsumer/consumer"
)

type healthcheck struct {
	client       http.Client
	consumerConf consumer.QueueConfig
}

func (h *healthcheck) healthcheck() func(w http.ResponseWriter, r *http.Request) {
	return fthealth.HandlerParallel("Dependent services healthcheck", "Checks if all the dependent services are reachable and healthy.", h.messageQueueProxyReachable())
}

func (h *healthcheck) gtg(writer http.ResponseWriter, req *http.Request) {
	healthChecks := []func() error{h.checkAggregateMessageQueueProxiesReachable}

	for _, hCheck := range healthChecks {
		if err := hCheck(); err != nil {
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
	}
}

func (h *healthcheck) messageQueueProxyReachable() fthealth.Check {
	return fthealth.Check{
		BusinessImpact:   "Publishing or updating videos will not be possible, clients will not see the new content.",
		Name:             "MessageQueueProxyReachable",
		PanicGuide:       "https://sites.google.com/a/ft.com/technology/systems/dynamic-semantic-publishing/extra-publishing/video-mapper",
		Severity:         1,
		TechnicalSummary: "Message queue proxy is not reachable/healthy",
		Checker:          h.checkAggregateMessageQueueProxiesReachable,
	}

}

func (h *healthcheck) checkAggregateMessageQueueProxiesReachable() error {
	errMsg := ""
	for i := 0; i < len(h.consumerConf.Addrs); i++ {
		err := h.checkMessageQueueProxyReachable(h.consumerConf.Addrs[i], h.consumerConf.Topic, h.consumerConf.AuthorizationKey)
		if err == nil {
			return nil
		}
		errMsg = errMsg + fmt.Sprintf("For %s there is an error %v \n", h.consumerConf.Addrs[i], err.Error())
	}
	return errors.New(errMsg)
}

func (h *healthcheck) checkMessageQueueProxyReachable(address string, topic string, authKey string) error {
	req, err := http.NewRequest("GET", address+"/topics", nil)
	if err != nil {
		warnLogger.Printf("Could not connect to proxy: %v", err.Error())
		return err
	}
	if len(authKey) > 0 {
		req.Header.Add("Authorization", authKey)
	}
	resp, err := h.client.Do(req)
	if err != nil {
		warnLogger.Printf("Could not connect to proxy: %v", err.Error())
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		errMsg := fmt.Sprintf("Proxy returned status: %d", resp.StatusCode)
		return errors.New(errMsg)
	}
	body, err := ioutil.ReadAll(resp.Body)
	return checkIfTopicIsPresent(body, topic)
}

func checkIfTopicIsPresent(body []byte, searchedTopic string) error {
	var topics []string
	err := json.Unmarshal(body, &topics)
	if err != nil {
		return fmt.Errorf("Error occured and topic could not be found. %v", err.Error())
	}
	for _, topic := range topics {
		if topic == searchedTopic {
			return nil
		}
	}
	return errors.New("Topic was not found")
}
