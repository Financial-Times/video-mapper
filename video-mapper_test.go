package main

import (
	"testing"
	"github.com/Financial-Times/message-queue-gonsumer/consumer"
)

func TestExtractUuid_NormalCase(t *testing.T) {
	var tests = []struct {
		message consumer.Message
		marshalledEvent string
	}{
		{
			consumer.Message{
				map[string]string{
					"X-Request-Id": "tid_123123",
					"Message-Timestamp":   "2016-04-29T11:02:58.304Z",
				},
				`{
				   "uuid":"bad50c54-76d9-30e9-8734-b999c708aa4c",
				   "id": "4492075574001",
				   "name": "test-video.mp4",
                                   "updated_at": "2015-09-17T17:41:20.782Z"
				}`,
			},
			`{` +
			  `"contentUri":"http://video-mapper-iw-uk-p.svc.ft.com/video/model/bad50c54-76d9-30e9-8734-b999c708aa4c",` +
			  `"payload":"{\"uuid\":\"bad50c54-76d9-30e9-8734-b999c708aa4c\",\"identifiers\":[{\"authority\":\"http://api.ft.com/system/BRIGHTCOVE\",\"identifierValue\":\"4492075574001\"}],\"publishedDate\":\"2015-09-17T17:41:20.782Z\",\"mediaType\":\"video/mp4\",\"publishReference\":\"tid_123123\",\"lastModified\":\"2016-04-29T11:02:58.304Z\"}",` +
			  `"lastModified":"2016-04-29T11:02:58.304Z"` +
			`}`,
		},
	}
	m := videoMapper{}
	for _, test := range tests {
		actualMarshalledEvent, err := m.mapMessage(test.message)
		if err != nil {
			t.Errorf("Error mapping message\n%v\n%v", test.message, err)
			continue
		}
		actualMarshalledEventS := string(actualMarshalledEvent)
		if actualMarshalledEventS != test.marshalledEvent {
			t.Errorf("Error mapping message\n%v\nExpected: %s\nActual: %s\n", test.message, test.marshalledEvent, actualMarshalledEventS)
		}
	}
}
