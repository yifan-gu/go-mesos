/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package messenger

import (
	"bytes"
	"fmt"
	"github.com/yifan-gu/go-mesos/upid"
	"io/ioutil"
	"net"
	"net/http"

	log "github.com/golang/glog"
)

// Transporter defines the interfaces that should be implemented.
type Transporter interface {
	Send(msg *Message) error
	Recv() *Message
	Install(messageName string)
	Start() error
	Stop() error
}

// HTTPTransporter implements the interfaces of the Transporter.
type HTTPTransporter struct {
	// If the host is empty("") then it will listen on localhost.
	// If the port is empty("") then it will listen on random port.
	UPID         *upid.UPID
	listener     net.Listener // TODO(yifan): Change to TCPListener.
	mux          *http.ServeMux
	client       *http.Client
	messageQueue chan *Message
}

// NewHTTPTransporter creates a new http transporter.
func NewHTTPTransporter(upid *upid.UPID) *HTTPTransporter {
	return &HTTPTransporter{
		UPID:         upid,
		messageQueue: make(chan *Message, defaultQueueSize),
		mux:          http.NewServeMux(),
		client:       new(http.Client),
	}
}

// Send sends the message to its specified upid.
func (t *HTTPTransporter) Send(msg *Message) error {
	log.V(2).Infof("Sending message to %v via http\n", msg.UPID)
	req, err := t.makeLibprocessRequest(msg)
	if err != nil {
		log.Errorf("Failed to make libprocess request: %v\n", err)
		return err
	}
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
func (t *HTTPTransporter) Install(msgName string) {
	requestURI := fmt.Sprintf("/%s/%s", t.UPID.ID, msgName)
	t.mux.HandleFunc(requestURI, t.messageHandler)
}

// Start starts the http transporter. This will block, should be put
// in a goroutine.
func (t *HTTPTransporter) Start() error {
	// NOTE: Explicitly specifis IPv4 because Libprocess
	// only supports IPv4 for now.
	ln, err := net.Listen("tcp4", net.JoinHostPort(t.UPID.Host, t.UPID.Port))
	if err != nil {
		return err
	}
	host, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		return err
	}
	t.UPID.Host, t.UPID.Port = host, port
	t.listener = ln

	if err := http.Serve(ln, t.mux); err != nil {
		return err
	}
	return nil
}

// Stop stops the http transporter by closing the listener.
func (t *HTTPTransporter) Stop() error {
	return t.listener.Close()
}

func (t *HTTPTransporter) messageHandler(w http.ResponseWriter, r *http.Request) {
	// TODO(yifan): Verify it's a libprocess request.
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Errorf("Failed to read HTTP body: %v\n", err)
		return
	}
	log.V(2).Infof("Receiving message from %v, length %v\n", r.RemoteAddr, len(data))
	t.messageQueue <- &Message{
		UPID:         nil,
		Name:         extractNameFromRequestURI(r.RequestURI),
		ProtoMessage: nil,
		Bytes:        data,
	}
}

func (t *HTTPTransporter) makeLibprocessRequest(msg *Message) (*http.Request, error) {
	hostport := net.JoinHostPort(msg.UPID.Host, msg.UPID.Port)
	targetURL := fmt.Sprintf("http://%s%s", hostport, msg.RequestPATH())
	req, err := http.NewRequest("POST", targetURL, bytes.NewReader(msg.Bytes))
	if err != nil {
		log.Errorf("Failed to create request: %v\n", err)
		return nil, err
	}
	req.Header.Add("Libprocess-From", t.UPID.String())
	return req, nil
}
