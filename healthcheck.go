package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/message-queue-gonsumer/consumer"
	"github.com/Financial-Times/service-status-go/gtg"
)

type healthcheck struct {
	client        http.Client
	consumerConf  consumer.QueueConfig
	appSystemCode string
}

func (h *healthcheck) createHC() fthealth.HC {
	return fthealth.HealthCheck{
		SystemCode:  h.appSystemCode,
		Description: "Maps video's native format to FT's Content structure.",
		Name:        "video-mapper",
		Checks:      []fthealth.Check{h.messageQueueCheck()},
	}
}

func (h *healthcheck) messageQueueCheck() fthealth.Check {
	return fthealth.Check{
		BusinessImpact:   "Publishing or updating videos will not be possible, clients will not see the new content.",
		Name:             "MessageQueueProxyReachable",
		PanicGuide:       "https://dewey.ft.com/up-vm.html",
		Severity:         1,
		TechnicalSummary: "Message queue proxy is not reachable/healthy",
		Checker:          h.checkAggregateMessageQueueProxiesReachable,
	}
}

func (h *healthcheck) gtgCheck() gtg.Status {
	msg, err := h.checkAggregateMessageQueueProxiesReachable()
	if err != nil {
		return gtg.Status{GoodToGo: false, Message: msg}
	}
	return gtg.Status{GoodToGo: true}
}

func (h *healthcheck) checkAggregateMessageQueueProxiesReachable() (string, error) {
	errMsg := ""
	for i := 0; i < len(h.consumerConf.Addrs); i++ {
		_, err := h.checkMessageQueueProxyReachable(h.consumerConf.Addrs[i], h.consumerConf.Topic, h.consumerConf.AuthorizationKey, h.consumerConf.Queue)
		if err == nil {
			return "Ok", nil
		}
		errMsg = errMsg + fmt.Sprintf("For %s there is an error %v \n", h.consumerConf.Addrs[i], err.Error())
	}
	return errMsg, errors.New(errMsg)
}

func (h *healthcheck) checkMessageQueueProxyReachable(address string, topic string, authKey string, queue string) (string, error) {
	req, err := http.NewRequest("GET", address+"/topics", nil)
	if err != nil {
		msg := fmt.Sprintf("Could not connect to proxy: %v", err.Error())
		warnLogger.Print(msg)
		return msg, err
	}
	if len(authKey) > 0 {
		req.Header.Add("Authorization", authKey)
	}
	if len(queue) > 0 {
		req.Host = queue
	}
	resp, err := h.client.Do(req)
	if err != nil {
		msg := fmt.Sprintf("Could not connect to proxy: %v", err.Error())
		warnLogger.Print(msg)
		return msg, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		errMsg := fmt.Sprintf("Proxy returned status: %d", resp.StatusCode)
		return errMsg, errors.New(errMsg)
	}
	body, err := ioutil.ReadAll(resp.Body)
	return checkIfTopicIsPresent(body, topic)
}

func checkIfTopicIsPresent(body []byte, searchedTopic string) (string, error) {
	var topics []string
	err := json.Unmarshal(body, &topics)
	if err != nil {
		msg := fmt.Sprintf("Error occured and topic could not be found. %v", err.Error())
		return msg, fmt.Errorf(msg)
	}
	for _, topic := range topics {
		if topic == searchedTopic {
			return "Ok", nil
		}
	}
	msg := "Topic was not found"
	return msg, errors.New(msg)
}
