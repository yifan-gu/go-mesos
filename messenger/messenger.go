package messenger

import (
	"fmt"
	"reflect"
	"time"

	"code.google.com/p/gogoprotobuf/proto"
	log "github.com/golang/glog"
)

const preparePeriod = time.Second * 1

// MessageHandler is the callback of the message.
type MessageHandler func(proto.Message)

// Messenger defines the interfaces that should be implemented.
type Messenger interface {
	Install(handler MessageHandler, msg proto.Message) error
	Send(upid *upid.UPID, msg proto.Message) error
	Start() error
	Stop()
}

// MesosMessenger is an implementation of the Messenger interface.
type MesosMessenger struct {
	upid              *upid.UPID
	inQueue           chan *message
	outQueue          chan *message
	installedMessages map[string]reflect.Type
	installedHandlers map[string]MessageHandler
	stop              chan struct{}
	tr                Transporter
}

// NewMesosMessenger creates a new mesos messenger.
func NewMesosMessenger(upid *upid.UPID) *MesosMessenger {
	return &MesosMessenger{
		upid:              upid,
		inQueue:           make(chan *Mmessage, defaultQueueSize),
		outQueue:          make(chan *Message, defaultQueueSize),
		installedMessages: make(map[string]reflect.Type),
		messageHandlers:   make(map[string]MessageHandler),
		stop:              make(chan struct{}),
		tr:                transporter.NewHTTPTransporter(upid),
	}
}

// Install installs the message with the given handler.
func (m *MesosMessenger) Install(msg proto.Message, handler MessageHandler) error {
	mtype := reflect.TypeOf(msg)
	name := getMessageName(msg)

	// Check if the message is already installed.
	if _, ok := m.installedMessages[name]; ok {
		err := fmt.Errorf("Message %v is already installed", name)
		log.Errorf("Failed to install message %v: %v\n", name, err)
		return err
	}
	m.installedMessages[name] = mtype
	m.installedHandlers[name] = handler
	m.tr.Install(name)
	return nil
}

// Send puts a message into the sending queue, waiting to be sent.
func (m *MesosMessenger) Send(upid upid.UPID, msg proto.Message) error {
	name := getMessageName(msg)
	if _, ok := m.installedMessages[name]; !ok {
		err := fmt.Errorf("Message %v is not installed", name)
		log.Errorf("Failed to send message %v: %v\n", name, err)
		return err
	}
	m.outQueue <- &Message{upid, name, msg, nil}
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

	go m.outgoingLoop()
	go m.decodingLoop()
	go m.incomingLoop()
	return nil
}

// Stop stops the messenger and clean up all the goroutines.
func (m *MesosMessenger) Stop() {
	m.tr.Stop()
	close(m.stop)
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
				log.Errorf("Failed to send message %v: %v\n", msg, err)
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
			m.installedHandlers[msg.Name](msg.ProtoMessage)
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
		msg.ProtoMessage := reflect.New(m.installedMessages[msg.Name]).Interface().(proto.Message)
		m.inQueue <- msg
	}
}

// getMessageName returns the name of the message in the mesos manner.
func getMessageName(msg proto.Message) string {
	return fmt.Sprintf("%v.%v", "mesos.internal", reflect.TypeOf(msg).Name())
}
