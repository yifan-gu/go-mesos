package messenger

import (
	"fmt"
	"reflect"
	"time"

	"code.google.com/p/gogoprotobuf/proto"
	log "github.com/golang/glog"
	"github.com/yifan-gu/messenger/transporter"
)

const preparePeriod = time.Second * 1

// MessageHandler is the callback of the message.
type MessageHandler func(proto.Message)

type messageRecord struct {
	handler  MessageHandler
	endpoint string
}

type message struct {
	hostport string
	endpoint string
	message  proto.Message
	data     []byte
}

// Messenger defines the interfaces that should be implemented.
type Messenger interface {
	Install(handler MessageHandler, msg proto.Message, endpoint string) error
	Send(hostport string, msg proto.Message) error
	Start() error
	Stop()
}

// MesosMessenger is an implementation of the Messenger interface.
type MesosMessenger struct {
	hostport          string
	inQueue           chan *message
	outQueue          chan *message
	installedMessages map[reflect.Type]*messageRecord
	endpointToHandler map[string]MessageHandler
	stop              chan struct{}
	tr                Transporter
}

// NewMesosMessenger creates a new mesos messenger.
func NewMesosMessenger(hostport string) *MesosMessenger {
	return &MesosMessenger{
		hostport:          hostport,
		inQueue:           make(chan *message, defaultQueueSize),
		outQueue:          make(chan *message, defaultQueueSize),
		installedMessages: make(map[reflect.Type]*messageRecord),
		endpointToHandler: make(map[string]MessageHandler),
		stop:              make(chan struct{}),
		tr:                transporter.NewHTTPTransporter(hostport),
	}
}

// Install installs the message with the given handler.
func (m *MesosMessenger) Install(handler MessageHandler, msg proto.Message, endpoint string) error {
	mtype := reflect.TypeOf(msg)
	// Check if the message is already installed.
	if _, ok := m.installedMessages[mtype]; ok {
		err := fmt.Errorf("Message %v is already installed", mtype)
		log.Errorf("Failed to install message: %v\n", err)
		return err
	}
	m.installedMessages[mtype] = &messageRecord{handler, endpoint}
	// Check if the endpoint is already installed.
	if mtype, ok := m.endpointToType[endpoint]; ok {
		err := fmt.Errorf("Endpoint is already installed with %v", mtype)
		log.Errorf("Failed to install message: %v\n", err)
		return err
	}
	m.endpointToType[endpoint] = mtype
	m.tr.Install(endpoint)
	return nil
}

// Send puts a message into the sending queue, waiting to be sent.
func (m *MesosMessenger) Send(hostport string, msg proto.Message) error {
	mtype := reflect.TypeOf(msg)
	rec, ok := m.installedMessages[mtype]
	if !ok {
		err := fmt.Errorf("Message %v is not installed", mtype)
		log.Errorf("Failed to send message: %v\n", err)
		return err
	}
	m.outQueue <- &message{hostport, rec.endpoint, msg}
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
			b, err := proto.Marshal(msg.message)
			if err != nil {
				log.Errorf("Failed to send message %v: %v\n", msg, err)
				continue
			}
			if err := m.tr.Send(msg.hostport, msg.endpoint, msg.data); err != nil {
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
			if err := proto.Unmarshal(msg.data, msg.message); err != nil {
				log.Errorf("Failed to unmarshal message %v: %v\n", msg, err)
				continue
			}
			// TODO(yifan): Catch panic.
			m.endpointToHandler[msg.endpoint](msg.message)
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
		b, hostport, endpoint := m.tr.Recv()
		msg := reflect.New(m.endpointToHandler[endpoint]).Interface().(proto.Message)
		m.inQueue <- &message{
			hostport: hostport,
			endpoint: endpoint,
			message:  msg,
			data:     b,
		}
	}
}
