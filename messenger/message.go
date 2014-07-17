package messenger

import (
	"strings"
)

// Message defines the type that passes in the Messenger.
type Message struct {
	UPID         *upid.UPID
	Name         string
	ProtoMessage proto.Message
	Bytes        []byte
}

// MakeRequestURI returns the request URI of the message.
func (m *Message) MakeRequestURI() string {
	return fmt.Sprintf("/%s/%s", m.UPID.ID(), m.Name)
}

// NOTE: This should not fail or panic.
func extractNameFromRequestURI(requestURI string) string {
	return strings.Split(requestURI, "/")[2]
}
