package main

import (
	"github.com/Financial-Times/message-queue-gonsumer/consumer"
	"os"
	"testing"
)

const longNativeJSON = `{
					  "uuid": "bad50c54-76d9-30e9-8734-b999c708aa4c",
					  "account_id": "1752604059001",
					  "complete": true,
					  "created_at": "2015-09-17T16:08:37.108Z",
					  "cue_points": [],
					  "custom_fields": {
					    "byline": "Waving sea"
					  },
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
					  "name": "Mediterranian: Storms",
					  "original_filename": "sea_marvels.mp4",
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
					  "published_at": "2015-09-17T17:41:20.782Z"
				}`

func TestExtractUuid_NormalCase(t *testing.T) {
	var tests = []struct {
		message         consumer.Message
		uuid            string
		marshalledEvent string
	}{
		{
			consumer.Message{
				Headers: map[string]string{
					"X-Request-Id":      "tid_123123",
					"Message-Timestamp": "2016-04-29T11:02:58.304Z",
				},
				Body: `{
				   "uuid":"bad50c54-76d9-30e9-8734-b999c708aa4c",
				   "id": "4492075574001",
				   "name": "Mediterranian: Storms",
				   "original_filename": "test-video.mp4",
                                   "published_at": "2015-09-17T17:41:20.782Z"
				}`,
			},
			"bad50c54-76d9-30e9-8734-b999c708aa4c",
			`{` +
				`"contentUri":"http://brightcove-video-model-mapper-iw-uk-p.svc.ft.com/video/model/bad50c54-76d9-30e9-8734-b999c708aa4c",` +
				`"payload":{"uuid":"bad50c54-76d9-30e9-8734-b999c708aa4c","title":"Mediterranian: Storms","identifiers":[{"authority":"http://api.ft.com/system/BRIGHTCOVE","identifierValue":"4492075574001"}],"brands":[{"id":"http://api.ft.com/things/dbb0bdae-1f0c-11e4-b0cb-b2227cce2b54"}],"publishedDate":"2015-09-17T17:41:20.782Z","mediaType":"video/mp4","publishReference":"tid_123123","lastModified":"2016-04-29T11:02:58.304Z","firstPublishedDate":"2015-09-17T17:41:20.782Z","canBeDistributed":"yes","body":"<body>video</body>"},` +
				`"lastModified":"2016-04-29T11:02:58.304Z"` +
				`}`,
		},
		{
			consumer.Message{
				Headers: map[string]string{
					"Message-Id":        "fba36a76-137a-4679-85b0-b9c3f95a3f08",
					"Message-Timestamp": "2016-04-29T10:59:39.914Z",
					"Message-Type":      "cms-content-published",
					"Origin-System-Id":  "http://cmdb.ft.com/systems/brightcove",
					"Content-Type":      "application/json",
					"X-Request-Id":      "tid_123123",
				},
				Body: longNativeJSON,
			},
			"bad50c54-76d9-30e9-8734-b999c708aa4c",
			`{` +
				`"contentUri":"http://brightcove-video-model-mapper-iw-uk-p.svc.ft.com/video/model/bad50c54-76d9-30e9-8734-b999c708aa4c",` +
				`"payload":{"uuid":"bad50c54-76d9-30e9-8734-b999c708aa4c","title":"Mediterranian: Storms","byline":"Waving sea","identifiers":[{"authority":"http://api.ft.com/system/BRIGHTCOVE","identifierValue":"4492075574001"}],"brands":[{"id":"http://api.ft.com/things/dbb0bdae-1f0c-11e4-b0cb-b2227cce2b54"}],"publishedDate":"2015-09-17T17:41:20.782Z","mediaType":"video/mp4","publishReference":"tid_123123","lastModified":"2016-04-29T10:59:39.914Z","firstPublishedDate":"2015-09-17T17:41:20.782Z","canBeDistributed":"yes","body":"<body>video</body>"},` +
				`"lastModified":"2016-04-29T10:59:39.914Z"` +
				`}`,
		},
		{
			consumer.Message{
				Headers: map[string]string{
					"X-Request-Id":      "tid_123123",
					"Message-Timestamp": "2016-04-29T11:02:58.304Z",
				},
				Body: `{
				   "uuid":"bad50c54-76d9-30e9-8734-b999c708aa4c",
				   "id": "4492075574001",
				   "name": "Mediterranian: Storms",
				   "original_filename": "test-video",
                                   "published_at": "2015-09-17T17:41:20.782Z"
				}`,
			},
			"bad50c54-76d9-30e9-8734-b999c708aa4c",
			`{` +
				`"contentUri":"http://brightcove-video-model-mapper-iw-uk-p.svc.ft.com/video/model/bad50c54-76d9-30e9-8734-b999c708aa4c",` +
				`"payload":{"uuid":"bad50c54-76d9-30e9-8734-b999c708aa4c","title":"Mediterranian: Storms","identifiers":[{"authority":"http://api.ft.com/system/BRIGHTCOVE","identifierValue":"4492075574001"}],"brands":[{"id":"http://api.ft.com/things/dbb0bdae-1f0c-11e4-b0cb-b2227cce2b54"}],"publishedDate":"2015-09-17T17:41:20.782Z","publishReference":"tid_123123","lastModified":"2016-04-29T11:02:58.304Z","firstPublishedDate":"2015-09-17T17:41:20.782Z","canBeDistributed":"yes","body":"<body>video</body>"},` +
				`"lastModified":"2016-04-29T11:02:58.304Z"` +
				`}`,
		},
		{
			consumer.Message{
				Headers: map[string]string{
					"X-Request-Id":      "tid_123123",
					"Message-Timestamp": "2016-04-29T11:02:58.304Z",
				},
				Body: `{
				   "uuid":"bad50c54-76d9-30e9-8734-b999c708aa4c",
				   "id": "4492075574001",
				   "name": "Mediterranian: Storms",
                                   "error_code": "RESOURCE_NOT_FOUND"
				}`,
			},
			"bad50c54-76d9-30e9-8734-b999c708aa4c",
			`{` +
				`"contentUri":"http://brightcove-video-model-mapper-iw-uk-p.svc.ft.com/video/model/bad50c54-76d9-30e9-8734-b999c708aa4c",` +
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

func TestIsPublishEvent_PublishedAtFieldPresent_PublishEventAndNoErr(t *testing.T) {
	video := map[string]interface{}{
		"id":           "4492075574001",
		"uuid":         "bad50c54-76d9-30e9-8734-b999c708aa4c",
		"published_at": "2015-09-17T16:08:37.108Z",
	}
	publishEvent, err := isPublishEvent(video)
	if err != nil {
		t.Errorf("Expected success. Found err: [%v]", err)
	}
	if !publishEvent {
		t.Error("Expected publish event")
	}
}

func TestIsPublishEvent_PublishedAtMissingErrorCodePresent_UnpublishEventAndNoErr(t *testing.T) {
	video := map[string]interface{}{
		"id":         "4492075574001",
		"uuid":       "bad50c54-76d9-30e9-8734-b999c708aa4c",
		"error_code": "RESOURCE_NOT_FOUND",
	}
	publishEvent, err := isPublishEvent(video)
	if err != nil {
		t.Errorf("Expected success. Found err: [%v]", err)
	}
	if publishEvent {
		t.Error("Expected unpublish event")
	}
}

func TestIsPublishEvent_PublishedAtAndErrorCodeBothMissing_ReturnError(t *testing.T) {
	video := map[string]interface{}{
		"id":   "4492075574001",
		"uuid": "bad50c54-76d9-30e9-8734-b999c708aa4c",
	}
	if _, err := isPublishEvent(video); err == nil {
		t.Error("Expected error")
	}
}

func TestBuildMediaType_FilenamesWithExtension_CorrectMediaType(t *testing.T) {
	var tests = []struct {
		filename string
		expected string
	}{
		{
			"foobar.mp4",
			"video/mp4",
		},
		{
			"foo/bar.avi",
			"video/avi",
		},
		{
			"foo.bar.mp4",
			"video/mp4",
		},
	}

	for _, test := range tests {
		actual, err := buildMediaType(test.filename)
		if err != nil {
			t.Errorf("Expected success. Found err: [%v]", err)
		}
		if actual != test.expected {
			t.Errorf("Expected: [%v]. Actual: [%v]", test.expected, actual)
		}
	}
}

func TestBuildMediaType_InvalidFilenames_ReturnError(t *testing.T) {
	var filenames = []string{"foobar", "foobar.", "foo.bar.", ""}

	for _, n := range filenames {
		actual, err := buildMediaType(n)
		if actual != "" {
			t.Errorf("Expected empty media type. Received: [%v]", actual)
		}
		if err == nil {
			t.Error("Expected error")
		}
	}
}

func TestGetPublishedDate(t *testing.T) {
	var testCases = []struct {
		video           map[string]interface{}
		expectedPubDate string
		expectErr       bool
	}{
		{
			map[string]interface{}{
				"published_at": "2015-09-17T16:08:37.108Z",
				"updated_at":   "2015-09-18T16:08:37.108Z",
			},
			"2015-09-17T16:08:37.108Z",
			false,
		},
		{
			map[string]interface{}{
				"published_at": nil,
				"updated_at":   "2015-09-18T16:08:37.108Z",
			},
			"2015-09-18T16:08:37.108Z",
			false,
		},
		{
			map[string]interface{}{
				"published_at": nil,
				"updated_at":   nil,
			},
			"",
			true,
		},
	}

	for _, tc := range testCases {
		pubDate, err := getPublishedDate(tc.video)
		if pubDate != tc.expectedPubDate {
			t.Errorf("Expected: pubDate: [%v]\nActual: pubDate: [%v]", tc.expectedPubDate, pubDate)
		}
		if tc.expectErr && err == nil || !tc.expectErr && err != nil {
			t.Errorf("Expect error [%v]. Actual error: [%v]", tc.expectErr, err)
		}
	}
}

func TestExtractBody(t *testing.T) {
	var tests = []struct {
		message         consumer.Message
		marshalledEvent string
	}{
		{
			consumer.Message{
				Headers: map[string]string{
					"Message-Id":        "fba36a76-137a-4679-85b0-b9c3f95a3f08",
					"Message-Timestamp": "2016-04-29T10:59:39.914Z",
					"Message-Type":      "cms-content-published",
					"Origin-System-Id":  "http://cmdb.ft.com/systems/brightcove",
					"Content-Type":      "application/json",
					"X-Request-Id":      "tid_123123",
				},
				Body: longNativeJSON,
			},
			`{` +
				`"contentUri":"http://brightcove-video-model-mapper-iw-uk-p.svc.ft.com/video/model/bad50c54-76d9-30e9-8734-b999c708aa4c",` +
				`"payload":{"uuid":"bad50c54-76d9-30e9-8734-b999c708aa4c","title":"Mediterranian: Storms","byline":"Waving sea","identifiers":[{"authority":"http://api.ft.com/system/BRIGHTCOVE","identifierValue":"4492075574001"}],"brands":[{"id":"http://api.ft.com/things/dbb0bdae-1f0c-11e4-b0cb-b2227cce2b54"}],"publishedDate":"2015-09-17T17:41:20.782Z","mediaType":"video/mp4","publishReference":"tid_123123","lastModified":"2016-04-29T10:59:39.914Z","firstPublishedDate":"2015-09-17T17:41:20.782Z","canBeDistributed":"yes","body":"<body>video</body>"},` +
				`"lastModified":"2016-04-29T10:59:39.914Z"` +
				`}`,
		},
		{
			consumer.Message{
				Headers: map[string]string{
					"X-Request-Id":      "tid_123123",
					"Message-Timestamp": "2016-04-29T11:02:58.304Z",
				},
				Body: `{
				   "uuid":"bad50c54-76d9-30e9-8734-b999c708aa4c",
				   "id": "4492075574001",
				   "description": "Jamil Anderlini looks for the real economy of Pyongyang",
				   "name": "Mediterranian: Storms",
				   "original_filename": "test-video",
                                   "published_at": "2015-09-17T17:41:20.782Z"
				}`,
			},
			`{` +
				`"contentUri":"http://brightcove-video-model-mapper-iw-uk-p.svc.ft.com/video/model/bad50c54-76d9-30e9-8734-b999c708aa4c",` +
				`"payload":{"uuid":"bad50c54-76d9-30e9-8734-b999c708aa4c","title":"Mediterranian: Storms","identifiers":[{"authority":"http://api.ft.com/system/BRIGHTCOVE","identifierValue":"4492075574001"}],"brands":[{"id":"http://api.ft.com/things/dbb0bdae-1f0c-11e4-b0cb-b2227cce2b54"}],"publishedDate":"2015-09-17T17:41:20.782Z","publishReference":"tid_123123","lastModified":"2016-04-29T11:02:58.304Z","firstPublishedDate":"2015-09-17T17:41:20.782Z","canBeDistributed":"yes","body":"<body>Jamil Anderlini looks for the real economy of Pyongyang</body>"},` +
				`"lastModified":"2016-04-29T11:02:58.304Z"` +
				`}`,
		},
		{
			consumer.Message{
				Headers: map[string]string{
					"X-Request-Id":      "tid_123123",
					"Message-Timestamp": "2016-04-29T11:02:58.304Z",
				},
				Body: `{
				   "uuid":"bad50c54-76d9-30e9-8734-b999c708aa4c",
				   "id": "4492075574001",
				   "name": "Mediterranian: Storms",
				   "original_filename": "test-video",
				   "long_description": "Jamil Anderlini looks for the real economy of Pyongyang",
                                   "published_at": "2015-09-17T17:41:20.782Z"
				}`,
			},
			`{` +
				`"contentUri":"http://brightcove-video-model-mapper-iw-uk-p.svc.ft.com/video/model/bad50c54-76d9-30e9-8734-b999c708aa4c",` +
				`"payload":{"uuid":"bad50c54-76d9-30e9-8734-b999c708aa4c","title":"Mediterranian: Storms","identifiers":[{"authority":"http://api.ft.com/system/BRIGHTCOVE","identifierValue":"4492075574001"}],"brands":[{"id":"http://api.ft.com/things/dbb0bdae-1f0c-11e4-b0cb-b2227cce2b54"}],"publishedDate":"2015-09-17T17:41:20.782Z","publishReference":"tid_123123","lastModified":"2016-04-29T11:02:58.304Z","firstPublishedDate":"2015-09-17T17:41:20.782Z","canBeDistributed":"yes","body":"<body>Jamil Anderlini looks for the real economy of Pyongyang</body>"},` +
				`"lastModified":"2016-04-29T11:02:58.304Z"` +
				`}`,
		},
		{
			consumer.Message{
				Headers: map[string]string{
					"X-Request-Id":      "tid_123123",
					"Message-Timestamp": "2016-04-29T11:02:58.304Z",
				},
				Body: `{
				   "uuid":"bad50c54-76d9-30e9-8734-b999c708aa4c",
				   "id": "4492075574001",
				   "name": "Mediterranian: Storms",
				   "original_filename": "test-video",
				   "description": "shouldnt be there",
				   "long_description": "Jamil Anderlini <looks> for the real economy of Pyongyang",
                                   "published_at": "2015-09-17T17:41:20.782Z"
				}`,
			},
			`{` +
				`"contentUri":"http://brightcove-video-model-mapper-iw-uk-p.svc.ft.com/video/model/bad50c54-76d9-30e9-8734-b999c708aa4c",` +
				`"payload":{"uuid":"bad50c54-76d9-30e9-8734-b999c708aa4c","title":"Mediterranian: Storms","identifiers":[{"authority":"http://api.ft.com/system/BRIGHTCOVE","identifierValue":"4492075574001"}],"brands":[{"id":"http://api.ft.com/things/dbb0bdae-1f0c-11e4-b0cb-b2227cce2b54"}],"publishedDate":"2015-09-17T17:41:20.782Z","publishReference":"tid_123123","lastModified":"2016-04-29T11:02:58.304Z","firstPublishedDate":"2015-09-17T17:41:20.782Z","canBeDistributed":"yes","body":"<body>Jamil Anderlini \u0026lt;looks\u0026gt; for the real economy of Pyongyang</body>"},` +
				`"lastModified":"2016-04-29T11:02:58.304Z"` +
				`}`,
		},
		{
			consumer.Message{
				Headers: map[string]string{
					"X-Request-Id":      "tid_123123",
					"Message-Timestamp": "2016-04-29T11:02:58.304Z",
				},
				Body: `{
				   "uuid":"bad50c54-76d9-30e9-8734-b999c708aa4c",
				   "id": "4492075574001",
				   "name": "Mediterranian: Storms",
				   "original_filename": "test-video",
                                   "published_at": "2015-09-17T17:41:20.782Z"
				}`,
			},
			`{` +
				`"contentUri":"http://brightcove-video-model-mapper-iw-uk-p.svc.ft.com/video/model/bad50c54-76d9-30e9-8734-b999c708aa4c",` +
				`"payload":{"uuid":"bad50c54-76d9-30e9-8734-b999c708aa4c","title":"Mediterranian: Storms","identifiers":[{"authority":"http://api.ft.com/system/BRIGHTCOVE","identifierValue":"4492075574001"}],"brands":[{"id":"http://api.ft.com/things/dbb0bdae-1f0c-11e4-b0cb-b2227cce2b54"}],"publishedDate":"2015-09-17T17:41:20.782Z","publishReference":"tid_123123","lastModified":"2016-04-29T11:02:58.304Z","firstPublishedDate":"2015-09-17T17:41:20.782Z","canBeDistributed":"yes","body":"<body>video</body>"},` +
				`"lastModified":"2016-04-29T11:02:58.304Z"` +
				`}`,
		},
	}
	m := videoMapper{}
	initLogs(os.Stdout, os.Stdout, os.Stderr)
	for _, test := range tests {
		actualMarshalledEvent, _, err := m.mapMessage(test.message)
		if err != nil {
			t.Errorf("Error mapping message\n%v\n%v", test.message, err.Error())
			continue
		}
		actualMarshalledEventS := string(actualMarshalledEvent)
		if actualMarshalledEventS != test.marshalledEvent {
			t.Errorf("Error mapping message\n%v\nExpected: %s\nActual: %s\n", test.message, test.marshalledEvent, actualMarshalledEventS)
		}
	}
}

func TestGetFirstPublishedDate(t *testing.T) {
	var testCases = []struct {
		video        map[string]interface{}
		expectedDate interface{}
	}{
		{
			map[string]interface{}{
				"published_at": "2015-09-17T16:08:37.108Z",
			},
			"2015-09-17T16:08:37.108Z",
		},
		{
			map[string]interface{}{
				"published_at": nil,
			},
			"",
		},
	}

	for _, tc := range testCases {
		firstPublishedDate := getFirstPublishedDate(tc.video)
		if firstPublishedDate != tc.expectedDate {
			t.Errorf("Expected: firstPublishedDate: [%v]\nActual: firstPublishedDate: [%v]", tc.expectedDate, firstPublishedDate)
		}
	}
}
