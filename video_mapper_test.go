package main

import (
	"github.com/Financial-Times/message-queue-gonsumer/consumer"
	"testing"
	"os"
)

func TestExtractUuid_NormalCase(t *testing.T) {
	var tests = []struct {
		message         consumer.Message
		uuid            string
		marshalledEvent string
	}{
		{
			consumer.Message{
				map[string]string{
					"X-Request-Id":      "tid_123123",
					"Message-Timestamp": "2016-04-29T11:02:58.304Z",
				},
				`{
				   "uuid":"bad50c54-76d9-30e9-8734-b999c708aa4c",
				   "id": "4492075574001",
				   "name": "test-video.mp4",
                                   "updated_at": "2015-09-17T17:41:20.782Z"
				}`,
			},
			"bad50c54-76d9-30e9-8734-b999c708aa4c",
			`{` +
				`"contentUri":"http://brightcove-video-model-mapper-iw-uk-p.svc.ft.com/video/model/bad50c54-76d9-30e9-8734-b999c708aa4c",` +
				`"payload":{"uuid":"bad50c54-76d9-30e9-8734-b999c708aa4c","identifiers":[{"authority":"http://api.ft.com/system/BRIGHTCOVE","identifierValue":"4492075574001"}],"publishedDate":"2015-09-17T17:41:20.782Z","mediaType":"video/mp4","publishReference":"tid_123123","lastModified":"2016-04-29T11:02:58.304Z"},` +
				`"lastModified":"2016-04-29T11:02:58.304Z"` +
				`}`,
		},
		{
			consumer.Message{
				map[string]string{
					"Message-Id":        "fba36a76-137a-4679-85b0-b9c3f95a3f08",
					"Message-Timestamp": "2016-04-29T10:59:39.914Z",
					"Message-Type":      "cms-content-published",
					"Origin-System-Id":  "http://cmdb.ft.com/systems/brightcove",
					"Content-Type":      "application/json",
					"X-Request-Id":      "tid_123123",
				},
				`{
					  "uuid": "bad50c54-76d9-30e9-8734-b999c708aa4c",
					  "account_id": "1752604059001",
					  "complete": true,
					  "created_at": "2015-09-17T16:08:37.108Z",
					  "cue_points": [],
					  "custom_fields": {},
					  "description": null,
					  "digital_master_id": "4492154733001",
					  "duration": 155573,
					  "economics": "AD_SUPPORTED",
					  "folder_id": null,
					  "geo": null,
					  "id": "4492075574001",
					  "images": {
					    "poster": {
					      "asset_id": "4492153571001",
					      "sources": [
						{
						  "src": "https://bcsecure01-a.akamaihd.net/6/1752604059001/201509/3164/1752604059001_4492153571001_4492075574001-vs.jpg?pubId=1752604059001&videoId=4492075574001"
						}
					      ],
					      "src": "https://bcsecure01-a.akamaihd.net/6/1752604059001/201509/3164/1752604059001_4492153571001_4492075574001-vs.jpg?pubId=1752604059001&videoId=4492075574001"
					    },
					    "thumbnail": {
					      "asset_id": "4492154714001",
					      "sources": [
						{
						  "src": "https://bcsecure01-a.akamaihd.net/6/1752604059001/201509/3164/1752604059001_4492154714001_4492075574001-th.jpg?pubId=1752604059001&videoId=4492075574001"
						}
					      ],
					      "src": "https://bcsecure01-a.akamaihd.net/6/1752604059001/201509/3164/1752604059001_4492154714001_4492075574001-th.jpg?pubId=1752604059001&videoId=4492075574001"
					    }
					  },
					  "link": null,
					  "long_description": null,
					  "name": "sea_marvels.mp4",
					  "reference_id": null,
					  "schedule": null,
					  "sharing": null,
					  "state": "ACTIVE",
					  "tags": [],
					  "text_tracks": [
					    {
					      "asset_id": "0cbd3425-8e94-46e6-9a10-a0d4491d4893",
					      "default": true,
					      "id": "c9001cee-d7f9-4b67-955c-9764cfc3d1f4",
					      "kind": "captions",
					      "label": null,
					      "mime_type": "text/vtt",
					      "sources": [
						{
						  "src": "https://bcsecure01-a.akamaihd.net/3/1752604059001/201509/3164/1752604059001_0cbd3425-8e94-46e6-9a10-a0d4491d4893_intro-vcs.vtt?pubId=1752604059001&videoId=4492075574001"
						}
					      ],
					      "src": "https://bcsecure01-a.akamaihd.net/3/1752604059001/201509/3164/1752604059001_0cbd3425-8e94-46e6-9a10-a0d4491d4893_intro-vcs.vtt?pubId=1752604059001&videoId=4492075574001",
					      "srclang": "en"
					    }
					  ],
					  "updated_at": "2015-09-17T17:41:20.782Z"
				}`,
			},
			"bad50c54-76d9-30e9-8734-b999c708aa4c",
			`{` +
				`"contentUri":"http://brightcove-video-model-mapper-iw-uk-p.svc.ft.com/video/model/bad50c54-76d9-30e9-8734-b999c708aa4c",` +
				`"payload":{"uuid":"bad50c54-76d9-30e9-8734-b999c708aa4c","identifiers":[{"authority":"http://api.ft.com/system/BRIGHTCOVE","identifierValue":"4492075574001"}],"publishedDate":"2015-09-17T17:41:20.782Z","mediaType":"video/mp4","publishReference":"tid_123123","lastModified":"2016-04-29T10:59:39.914Z"},` +
				`"lastModified":"2016-04-29T10:59:39.914Z"` +
				`}`,
		},
		{
			consumer.Message{
				map[string]string{
					"X-Request-Id":      "tid_123123",
					"Message-Timestamp": "2016-04-29T11:02:58.304Z",
				},
				`{
				   "uuid":"bad50c54-76d9-30e9-8734-b999c708aa4c",
				   "id": "4492075574001",
				   "name": "test-video",
                                   "updated_at": "2015-09-17T17:41:20.782Z"
				}`,
			},
			"bad50c54-76d9-30e9-8734-b999c708aa4c",
			`{` +
				`"contentUri":"http://brightcove-video-model-mapper-iw-uk-p.svc.ft.com/video/model/bad50c54-76d9-30e9-8734-b999c708aa4c",` +
				`"payload":{"uuid":"bad50c54-76d9-30e9-8734-b999c708aa4c","identifiers":[{"authority":"http://api.ft.com/system/BRIGHTCOVE","identifierValue":"4492075574001"}],"publishedDate":"2015-09-17T17:41:20.782Z","publishReference":"tid_123123","lastModified":"2016-04-29T11:02:58.304Z"},` +
				`"lastModified":"2016-04-29T11:02:58.304Z"` +
				`}`,
		},
	}
	m := videoMapper{}
	initLogs(os.Stdout, os.Stdout, os.Stderr)
	for _, test := range tests {
		actualMarshalledEvent, actualUUID, err := m.mapMessage(test.message)
		if err != nil {
			t.Errorf("Error mapping message\n%v\n%v", test.message, err.Error())
			continue
		}
		actualMarshalledEventS := string(actualMarshalledEvent)
		if actualMarshalledEventS != test.marshalledEvent {
			t.Errorf("Error mapping message\n%v\nExpected: %s\nActual: %s\n", test.message, test.marshalledEvent, actualMarshalledEventS)
		}
		if actualUUID != test.uuid {
			t.Errorf("Error retrieving uuid\n%v\nExpected: %s\nActual: %s\n", test.message, test.uuid, actualUUID)
		}
	}
}
