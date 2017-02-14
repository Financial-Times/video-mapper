package consumer

import (
	"net/http"
	"time"
	"log"
)

type AgeingClient struct {
	Client http.Client
	MaxAge time.Duration
}

func (client AgeingClient) StartAgeingProcess() {
	log.Printf("INFO: Starting aging [%d]", client.MaxAge)
	ticker := time.NewTicker(client.MaxAge)
	go func() {
		for {
			select {
			case <-ticker.C:
				log.Print("INFO: Closing idle connections")
				client.Client.Transport.(*http.Transport).CloseIdleConnections()
			}
		}
	}()
}
