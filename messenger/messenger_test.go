package messenger

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"code.google.com/p/gogoprotobuf/proto"
	"github.com/mesosphere/testify/assert"
	"github.com/yifan-gu/go-mesos/messenger/testmessage"
	"github.com/yifan-gu/go-mesos/upid"
)

const messageTypes = 4

func generateMessages(n int) []proto.Message {
	queue := make([]proto.Message, n*4)
	for i := 0; i < n*messageTypes; i = i + messageTypes {
		queue[i] = &testmessage.GoGoProtobufTestMessage1{
			F0: proto.Int32(int32(rand.Int())),
			F1: proto.String(fmt.Sprintf("%10d", rand.Int())),
			F2: proto.Float32(rand.Float32()),
		}
		queue[i+1] = &testmessage.GoGoProtobufTestMessage2{
			F0: proto.Int32(int32(rand.Int())),
			F1: proto.String(fmt.Sprintf("%10d", rand.Int())),
			F2: proto.Float32(rand.Float32()),
		}
		queue[i+2] = &testmessage.GoGoProtobufTestMessage3{
			F0: proto.Int32(int32(rand.Int())),
			F1: proto.String(fmt.Sprintf("%10d", rand.Int())),
			F2: proto.String(fmt.Sprintf("%10d", rand.Int())),
		}
		queue[i+3] = &testmessage.GoGoProtobufTestMessage4{
			F0: proto.Int32(int32(rand.Int())),
			F1: proto.String(fmt.Sprintf("%10d", rand.Int())),
		}
	}
	// Shuffle the messages.
	for i := range queue {
		index := rand.Intn(i + 1)
		queue[i], queue[index] = queue[index], queue[i]
	}
	return queue
}

func installMessages(t *testing.T, m Messenger, queue *[]proto.Message, counts *[]int, done chan struct{}) {
	testCounts := func(counts []int, done chan struct{}) {
		for i := range counts {
			if counts[i] != cap(*queue)/messageTypes {
				return
			}
		}
		close(done)
	}
	hander1 := func(from *upid.UPID, pbMsg proto.Message) {
		(*queue) = append(*queue, pbMsg)
		(*counts)[0]++
		testCounts(*counts, done)
	}
	hander2 := func(from *upid.UPID, pbMsg proto.Message) {
		(*queue) = append(*queue, pbMsg)
		(*counts)[1]++
		testCounts(*counts, done)
	}
	hander3 := func(from *upid.UPID, pbMsg proto.Message) {
		(*queue) = append(*queue, pbMsg)
		(*counts)[2]++
		testCounts(*counts, done)
	}
	hander4 := func(from *upid.UPID, pbMsg proto.Message) {
		(*queue) = append(*queue, pbMsg)
		(*counts)[3]++
		testCounts(*counts, done)
	}
	assert.NoError(t, m.Install(hander1, &testmessage.GoGoProtobufTestMessage1{}))
	assert.NoError(t, m.Install(hander2, &testmessage.GoGoProtobufTestMessage2{}))
	assert.NoError(t, m.Install(hander3, &testmessage.GoGoProtobufTestMessage3{}))
	assert.NoError(t, m.Install(hander4, &testmessage.GoGoProtobufTestMessage4{}))
}

func TestMessengerFailToInstall(t *testing.T) {
	m := NewMesosMessenger(&upid.UPID{ID: "mesos"})
	handler := func(from *upid.UPID, pbMsg proto.Message) {}
	assert.NotNil(t, m)
	assert.NoError(t, m.Install(handler, &testmessage.GoGoProtobufTestMessage1{}))
	assert.Error(t, m.Install(handler, &testmessage.GoGoProtobufTestMessage1{}))
}

func TestMessengerFailToStart(t *testing.T) {
	m1 := NewMesosMessenger(&upid.UPID{ID: "mesos", Host: "localhost", Port: "5050"})
	m2 := NewMesosMessenger(&upid.UPID{ID: "mesos", Host: "localhost", Port: "5050"})
	assert.NoError(t, m1.Start())
	assert.Error(t, m2.Start())
}

func TestMessengerFailToSend(t *testing.T) {
	upid, err := upid.Parse("mesos1@localhost:5051")
	assert.NoError(t, err)
	m := NewMesosMessenger(upid)
	assert.NoError(t, m.Start())
	assert.Error(t, m.Send(upid, &testmessage.GoGoProtobufTestMessage1{}))
}

func TestMessenger(t *testing.T) {
	messages := generateMessages(1000)

	upid1, err := upid.Parse("mesos1@localhost:5052")
	assert.NoError(t, err)
	upid2, err := upid.Parse("mesos2@localhost:5053")
	assert.NoError(t, err)

	m1 := NewMesosMessenger(upid1)
	m2 := NewMesosMessenger(upid2)

	counts := make([]int, messageTypes)
	msgQueue := make([]proto.Message, 0, len(messages))
	done := make(chan struct{})
	installMessages(t, m2, &msgQueue, &counts, done)

	assert.NoError(t, m1.Start())
	assert.NoError(t, m2.Start())

	go func() {
		for _, msg := range messages {
			assert.NoError(t, m1.Send(upid2, msg))
		}
	}()

	select {
	case <-time.After(time.Second * 10):
		t.Fatalf("Timeout")
	case <-done:
	}

	for i := range counts {
		assert.Equal(t, 1000, counts[i])
	}
	assert.Equal(t, messages, msgQueue)
}
