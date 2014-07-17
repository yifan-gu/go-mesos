package upid

import (
	"fmt"
	"strconv"
	"strings"
)

// UPID is a equivalent of the UPID in libprocess.
type UPID struct {
	id   string
	ip   uint32
	port uint16
}

// Parse parses the UPID from the input string.
func Parse(input string) (*UPID, error) {
	upid := new(UPID)
	splits := strings.Split(input, "@")
	if len(splits) != 2 {
		return nil, fmt.Errorf("Expect one `@' in the input")
	}
	upid.id = splits[0]
	hostport := strings.Split(splits[1], ":")
	if len(hostport) != 2 {
		return nil, fmt.Errorf("Expect one `:' in the hostport")
	}
	ip, err := strconv.Atoi(hostport[0])
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(hostport[1])
	if err != nil {
		return nil, err
	}
	upid.ip, upid.port = uint32(ip), uint16(port)
	return upid, nil
}

// String returns the string representation.
func (u *UPID) String() string {
	return fmt.Sprintf("%v@%v:%v", id, ip, port)
}

// Hostport returns the string representation of the UPID's ip and port.
func (u *UPID) Hostport() string {
	return fmt.Sprintf("%v:%v", u.ip, u.port)
}

// Ip returns the string representation of the UPID's ip.
func (u *UPID) IP() string {
	return fmt.Sprintf("%v", u.ip)
}

// Port returns the string representation of the UPID's port.
func (u *UPID) Port() string {
	return fmt.Sprintf("%v", u.port)
}

// ID returns the string representation of the UPID's id.
func (u *UPID) ID() string {
	return fmt.Sprintf("%v", u.Id)
}
