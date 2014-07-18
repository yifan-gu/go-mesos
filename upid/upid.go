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

package upid

import (
	"fmt"
	"strings"
)

// UPID is a equivalent of the UPID in libprocess.
type UPID struct {
	ID   string
	Host string
	Port string
}

// Parse parses the UPID from the input string.
func Parse(input string) (*UPID, error) {
	upid := new(UPID)
	splits := strings.Split(input, "@")
	if len(splits) != 2 {
		return nil, fmt.Errorf("Expect one `@' in the input")
	}
	upid.ID = splits[0]
	hostport := strings.Split(splits[1], ":")
	if len(hostport) != 2 {
		return nil, fmt.Errorf("Expect one `:' in the hostport")
	}
	// TODO(yifan): Validate the host:port's format.
	upid.Host, upid.Port = hostport[0], hostport[1]
	return upid, nil
}

// String returns the string representation.
func (u *UPID) String() string {
	return fmt.Sprintf("%s@%s:%s", u.ID, u.Host, u.Port)
}
