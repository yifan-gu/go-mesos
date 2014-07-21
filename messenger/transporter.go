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
	"strings"

	log "github.com/golang/glog"
)

// Transporter defines the interfaces that should be implemented.
type Transporter interface {
	Send(msg *Message) error
	Recv() *Message
	Install(messageName string)
	Start() error
	Stop() error
	UPID() *upid.UPID
}

// HTTPTransporter implements the interfaces of the Transporter.
type HTTPTransporter struct {
	// If the host is empty("") then it will listen on localhost.
	// If the port is empty("") then it will listen on random port.
	upid         *upid.UPID
	listener     net.Listener // TODO(yifan): Change to TCPListener.
	mux          *http.ServeMux
	client       *http.Client // TODO(yifan): Set read/write deadline.
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
	requestURI := fmt.Sprintf("/%s/%s", t.upid.ID, msgName)
	t.mux.HandleFunc(requestURI, t.messageHandler)
}

// Start starts the http transporter. This will block, should be put
// in a goroutine.
func (t *HTTPTransporter) Start() error {
	// NOTE: Explicitly specifis IPv4 because Libprocess
	// only supports IPv4 for now.
	ln, err := net.Listen("tcp4", net.JoinHostPort(t.upid.Host, t.upid.Port))
	if err != nil {
		return err
	}
	host, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		return err
	}
	t.upid.Host, t.upid.Port = host, port
	t.listener = ln

	// TODO(yifan): Set read/write deadline.
	if err := http.Serve(ln, t.mux); err != nil {
		return err
	}
	return nil
}

// Stop stops the http transporter by closing the listener.
func (t *HTTPTransporter) Stop() error {
	return t.listener.Close()
}

// UPID returns the upid of the transporter.
func (t *HTTPTransporter) UPID() *upid.UPID {
	return t.upid
}

func (t *HTTPTransporter) messageHandler(w http.ResponseWriter, r *http.Request) {
	// Verify it's a libprocess request.
	from, err := getLibprocessFrom(r)
	if err != nil {
		log.Errorf("Ignoring the request, because it's not a libprocess request: %v\n", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Errorf("Failed to read HTTP body: %v\n", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	log.V(2).Infof("Receiving message from %v, length %v\n", from, len(data))
	upid, err := upid.Parse(from)
	if err != nil {
		log.Errorf("Failed to parse libprocess-from %v: %v\n", from, err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusAccepted)
	t.messageQueue <- &Message{
		UPID:         upid,
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
	req.Header.Add("Libprocess-From", t.upid.String())
	return req, nil
}

// TODO(yifan): Refactor this.
func getLibprocessFrom(r *http.Request) (string, error) {
	if r.Method == "POST" {
		ua, ok := r.Header["User-Agent"]
		if len(ua) > 1 {
			return "", fmt.Errorf("Only support one user-agnent for now")
		}
		if ok && strings.HasPrefix(ua[0], "libprocess/") {
			return ua[0][len("libprocess/"):], nil
		}
		lf, ok := r.Header["Libprocess-From"]
		if len(lf) > 1 {
			return "", fmt.Errorf("Only support one libprocess-from for now")
		}
		if ok {
			return lf[0], nil
		}
	}
	return "", fmt.Errorf("Cannot determine libprocess from")
}
