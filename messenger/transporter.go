package messenger

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"

	log "github.com/golang/glog"
)

// Transporter defines the interfaces that should be implemented.
type Transporter interface {
	Send(msg *Message) error
	Recv() *Message
	Install(messageName string)
	Start() error
	Stop()
}

// HTTPTransporter implements the interfaces of the Transporter.
type HTTPTransporter struct {
	upid         *upid.UPID
	mux          *http.ServeMux
	client       *http.Client
	messageQueue chan *Message
}

// NewHTTPTransporter creates a new http transporter.
func NewHTTPTransporter(upid *upid.UPID) *HTTPTransporter {
	return &HTTPTransporter{
		upid:         upid,
		messageQueue: make(chan *Message, defaultQueueSize),
		mux:          http.NewServeMux(),
		client:       new(http.Client),
	}
}

// Send sends the message to its specified upid.
func (t *HTTPTransporter) Send(msg *Message) error {
	log.V(2).Infof("Sending message to %v\n", msg.UPID)
	req := t.makeLibprocessRequest(msg)
	resp, err := t.client.Do(req)
	if err != nil {
		log.Errorf("Failed to POST: %v\n", err)
		return err
	}
	defer resp.Body.Close()
	return nil
}

// Recv returns the message, one at a time.
func (t *HTTPTransporter) Recv() *Message {
	return <-t.messageQueue
}

// Install the request URI according to the message's name.
func (t *HTTPTransporter) Install(msg *Message) {
	t.mux.HandleFunc(msg.MakeRequestURI(), t.messageHandler)
}

// Start starts the http transporter. This will block, should be put
// in a goroutine.
func (t *HTTPTransporter) Start() error {
	if err := http.ListenAndServe(t.upid.Hostport(), t.mux); err != nil {
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
	t.messageQueue <- &Message{
		UPID:         upid,
		Name:         extractNameFromRequestURI(r.RequestURI),
		ProtoMessage: nil,
		Bytes:        data,
	}
}

func (t *HTTPTransporter) makeLibprocessRequest(msg *Message) (*http.Request, error) {
	targetURL := fmt.Sprintf("http://%s%s", msg.upid.Hostport(), msg.MakeRequestURI())
	url, err := url.Parse(targetURL)
	if err != nil {
		log.Errorf("Faild to parse url %v: %v\n", targetURL, url)
		return nil, err
	}
	req := &http.Request{
		Method: "POST",
		URL:    url,
		Header: map[string][]string{
			"User-Agent": {t.upid.String()},
		},
	}
	return req, nil
}
