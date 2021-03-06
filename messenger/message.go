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
	"strings"

	"code.google.com/p/gogoprotobuf/proto"
	"github.com/yifan-gu/go-mesos/upid"
)

// Message defines the type that passes in the Messenger.
type Message struct {
	UPID         *upid.UPID
	Name         string
	ProtoMessage proto.Message
	Bytes        []byte
}

// RequestURI returns the request URI of the message.
func (m *Message) RequestURI() string {
	return fmt.Sprintf("/%s/%s", m.UPID.ID, m.Name)
}

// NOTE: This should not fail or panic.
func extractNameFromRequestURI(requestURI string) string {
	return strings.Split(requestURI, "/")[2]
}
