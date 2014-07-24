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
	"fmt"
	"reflect"
	"time"

	"code.google.com/p/gogoprotobuf/proto"
	log "github.com/golang/glog"
	"github.com/yifan-gu/go-mesos/upid"
)

const (
	defaultQueueSize = 1024
	preparePeriod    = time.Second * 1
)

// MessageHandler is the callback of the message. When the callback
// is invoked, the sender's upid and the message is passed to the callback.
type MessageHandler func(from *upid.UPID, pbMsg proto.Message)

// Messenger defines the interfaces that should be implemented.
type Messenger interface {
	Install(handler MessageHandler, msg proto.Message) error
	Send(upid *upid.UPID, msg proto.Message) error
	Start() error
	Stop()
	UPID() *upid.UPID
}

// MesosMessenger is an implementation of the Messenger interface.
type MesosMessenger struct {
	upid              *upid.UPID
	inQueue           chan *Message
	outQueue          chan *Message
	installedMessages map[string]reflect.Type
	installedHandlers map[string]MessageHandler
	stop              chan struct{}
	tr                Transporter
}

// NewMesosMessenger creates a new mesos messenger.
func NewMesosMessenger(upid *upid.UPID) *MesosMessenger {
	return &MesosMessenger{
		upid:              upid,
		inQueue:           make(chan *Message, defaultQueueSize),
		outQueue:          make(chan *Message, defaultQueueSize),
		installedMessages: make(map[string]reflect.Type),
		installedHandlers: make(map[string]MessageHandler),
		stop:              make(chan struct{}),
		tr:                NewHTTPTransporter(upid),
	}
}

// Install installs the handler with the given message.
func (m *MesosMessenger) Install(handler MessageHandler, msg proto.Message) error {
	// Check if the message is a pointer.
	mtype := reflect.TypeOf(msg)
	if mtype.Kind() != reflect.Ptr {
		return fmt.Errorf("Message %v is not a Ptr type")
	}

	// Check if the message is already installed.
	name := getMessageName(msg)
	if _, ok := m.installedMessages[name]; ok {
		return fmt.Errorf("Message %v is already installed", name)
	}
	m.installedMessages[name] = mtype.Elem()
	m.installedHandlers[name] = handler
	m.tr.Install(name)
	return nil
}

// Send puts a message into the sending queue, waiting to be sent.
// With buffered channels, this will not block under moderate throughput.
// So there is no need to fire a goroutine each time to send a message,
// but we need to verify this later.
func (m *MesosMessenger) Send(upid *upid.UPID, msg proto.Message) error {
	if upid.Equal(m.upid) {
		return fmt.Errorf("Send the message to self")
	}
	name := getMessageName(msg)
	log.Infof("Sending message %v to %v\n", name, upid)
	m.outQueue <- &Message{upid, name, msg, nil}
	return nil
}

// Start starts the messenger.
func (m *MesosMessenger) Start() error {
	errChan := make(chan error)
	go func() {
		if err := m.tr.Start(); err != nil {
			errChan <- err
		}
	}()

	select {
	case err := <-errChan:
		return err
	case <-time.After(preparePeriod):
	}

	m.upid = m.tr.UPID()

	go m.outgoingLoop()
	go m.decodingLoop()
	go m.incomingLoop()
	return nil
}

// Stop stops the messenger and clean up all the goroutines.
func (m *MesosMessenger) Stop() {
	if err := m.tr.Stop(); err != nil {
		log.Errorf("Failed to stop the transporter: %v\n", err)
	}
	close(m.stop)
}

// UPID returns the upid of the messenger.
func (m *MesosMessenger) UPID() *upid.UPID {
	return m.upid
}

func (m *MesosMessenger) outgoingLoop() {
	for {
		select {
		case <-m.stop:
			return
		case msg := <-m.outQueue:
			b, err := proto.Marshal(msg.ProtoMessage)
			if err != nil {
				log.Errorf("Failed to send message %v: %v\n", msg, err)
				continue
			}
			msg.Bytes = b
			if err := m.tr.Send(msg); err != nil {
				log.Errorf("Failed to send message %v: %v\n", msg.Name, err)
				continue
			}
		}
	}
}

// From the queue to the callbacks
func (m *MesosMessenger) decodingLoop() {
	for {
		select {
		case <-m.stop:
			return
		case msg := <-m.inQueue:
			if err := proto.Unmarshal(msg.Bytes, msg.ProtoMessage); err != nil {
				log.Errorf("Failed to unmarshal message %v: %v\n", msg, err)
				continue
			}
			// TODO(yifan): Catch panic.
			m.installedHandlers[msg.Name](msg.UPID, msg.ProtoMessage)
		}
	}
}

// Feed the message to decodingLoop. Use a channel as a buffer in case
// the transporter implementation doesn't provide any buffers.
func (m *MesosMessenger) incomingLoop() {
	for {
		select {
		case <-m.stop:
			return
		default:
		}
		msg := m.tr.Recv()
		msg.ProtoMessage = reflect.New(m.installedMessages[msg.Name]).Interface().(proto.Message)
		m.inQueue <- msg
	}
}

// getMessageName returns the name of the message in the mesos manner.
func getMessageName(msg proto.Message) string {
	return fmt.Sprintf("%v.%v", "mesos.internal", reflect.TypeOf(msg).Elem().Name())
}
