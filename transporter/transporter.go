package transporter

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"

	log "github.com/golang/glog"
)

// Transporter defines the interfaces that should be implemented.
type Transporter interface {
	Send(hostport, endpoint string, data []byte) error
	Recv() (data []byte, hostport, endpoint string)
	Install(endpoint string)
	Start() error
	Stop()
}

// HTTPTransporter implements the interfaces of the Transporter.
type HTTPTransporter struct {
	hostport        string // hostport for the local socket.
	mux             *http.ServeMux
	client          *http.Client
	rawMessageQueue chan *rawMessage
}

// NewHTTPTransporter creates a new http transporter.
func NewHTTPTransporter(hostport string) *HTTPTransporter {
	return &HTTPTransporter{
		hostport:        hostport,
		rawMessageQueue: make(chan *rawMessage),
		mux:             http.NewServeMux(),
		client:          new(http.Client),
	}
}

// Send sends the data to the endpoint on the hostport.
func (t *HTTPTransporter) Send(hostport, endpoint string, data []byte) error {
	targetURL := fmt.Sprintf("http://%s%s", hostport, endpoint)
	log.V(2).Infof("Sending message to %v\n", hostport)
	// TODO(yifan): Use client.Do
	resp, err := t.client.Post(targetURL, "something", bytes.NewReader(data))
	if resp == nil || err != nil {
		log.Errorf("Failed to POST: %v\n", err)
		return err
	}
	defer resp.Body.Close()
	return nil
}

// Recv returns the data with the sender's hostport and the endpoint.
// One at a time.
func (t *HTTPTransporter) Recv() (data []byte, hostport, endpoint string) {
	msg := <-t.rawMessageQueue
	return msg.data, msg.hostport, msg.endpoint
}

// Install the endpoint.
func (t *HTTPTransporter) Install(endpoint string) {
	t.mux.HandleFunc(endpoint, t.messageHandler)
}

// Start starts the http transporter. This will block, should be put
// in a goroutine.
func (t *HTTPTransporter) Start() error {
	if err := http.ListenAndServe(t.hostport, t.mux); err != nil {
		return err
	}
	return nil
}

// Stop is a no-op for http transporter for now.
func (t *HTTPTransporter) Stop() {}

func (t *HTTPTransporter) messageHandler(w http.ResponseWriter, r *http.Request) {
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Errorf("Failed to read HTTP body: %v\n", err)
		return
	}
	log.V(2).Infof("Receiving message from %v\n", r.RemoteAddr)
	t.rawMessageQueue <- &rawMessage{data, r.RemoteAddr, r.RequestURI}
}
