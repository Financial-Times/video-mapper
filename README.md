# video-mapper
[![Circle CI](https://circleci.com/gh/Financial-Times/video-mapper/tree/master.png?style=shield)](https://circleci.com/gh/Financial-Times/video-mapper/tree/master)

Listens to kafka(-bridge) NativeCmsPublicationEvents, gets videos, transforms them to Content, puts them back to kafka(-bridge) to CmsPublicationEvents.

## Binary run

```
export Q_ADDR="http://172.23.53.64:8080" ## you'll have to change the address to your queue
export Q_GROUP=videoMapper
export Q_READ_TOPIC=NativeCmsPublicationEvents
export Q_READ_QUEUE=kafka
export Q_WRITE_TOPIC=CmsPublicationEvents
export Q_WRITE_QUEUE=kafka
export Q_AUTHORIZATION=$(etcdctl get /ft/_credentials/kafka-bridge/authorization_key) ## this is not exact, you'll have to get it from the cluster's etcd
go build
./video-mapper
```

## Docker build

`docker built -t video-mapper .`

## Run with docker example
```
export Q_AUTHORIZATION=$(etcdctl get /ft/_credentials/kafka-bridge/authorization_key)

docker run -p 8080 \
    --env "Q_ADDR=http://172.23.53.64:8080" \
    --env "Q_GROUP=videoMapper" \
    --env "Q_READ_TOPIC=NativeCmsPublicationEvents" \
    --env "Q_READ_QUEUE=kafka" \
    --env "Q_WRITE_TOPIC=CmsPublicationEvents" \
    --env "Q_WRITE_QUEUE=kafka" \
    --env "Q_AUTHORIZATION=$Q_AUTHORIZATION"
    video-mapper
```

## Verify the mapping logic via HTTP

Take a native video json format from [this gist](https://gist.github.com/kovacshuni/d16077e084d6fb3dc0aec6d6ee4239a5#file-message-on-nativecmspublicationevents-txt)
Just the JSON, not the headers.

HTTP POST it to this app's `/map` endpoint.

You should receive a response body like:
```
{
  "contentUri": "http://brightcove-video-mapper-iw-uk-p.svc.ft.com/video/model/bad50c54-76d9-30e9-8734-b999c708aa4c",
  "payload": {
    "uuid": "bad50c54-76d9-30e9-8734-b999c708aa4c",
    "identifiers": [
      {
        "authority": "http://api.ft.com/system/BRIGHTCOVE",
        "identifierValue": "4492075574001"
      }
    ],
    "publishedDate": "2016-04-29T11:02:58.000Z",
    "mediaType": "video/mp4",
    "publishReference": "tid_123123",
    "lastModified": "2016-04-29T11:02:58.269Z"
  },
  "lastModified": "2016-04-29T11:02:58.269Z"
}
```

This is just for verification, does not send the message to any queue.
