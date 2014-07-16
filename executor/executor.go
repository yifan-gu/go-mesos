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

package executor

import (
	"fmt"
	"github.com/go-distributed/messenger"
	"github.com/yifan-gu/go-mesos/mesosproto"
)

// Executor interface defines all the functions that are needed to implement
// a mesos executor.
type Executor interface {
	Registered(ExecutorDriver, *mesosproto.ExecutorInfo, *mesosproto.FrameworkInfo, *mesosproto.SlaveInfo)
	Reregistered(ExecutorDriver, *mesosproto.SlaveInfo)
	Disconnected(ExecutorDriver)
	LaunchTask(ExecutorDriver, *mesosproto.TaskInfo)
	KillTask(ExecutorDriver, *mesosproto.TaskID)
	FrameworkMessage(ExecutorDriver, string)
	Shutdown(ExecutorDriver)
	Error(ExecutorDriver, string)
}

// ExecutorDriver interface defines the functions that are needed to implement
// a mesos executor driver.
type ExecutorDriver interface {
	Init() error
	Start() error
	Stop() error
	Abort() error
	Join() error
	Run() error
	SendStatusUpdate(*TaskStatus) error
	SendFrameworkMessage(string) error
	Destroy() error
}

// MesosExecutorDriver is a implementation of the ExecutorDriver.
type MesosExecutorDriver struct {
	executor  Executor
	messenger messenger.Messenger
}

// Init inits the driver.
func (e *MesosExecutorDriver) Init() error {
	return fmt.Errorf("Not implemented")
}

// Start starts the driver.
func (e *MesosExecutorDriver) Start() error {
	return fmt.Errorf("Not implemented")
}

// Stop stops the driver.
func (e *MesosExecutorDriver) Stop() error {
	return fmt.Errorf("Not implemented")
}

// Abort aborts the driver.
func (e *MesosExecutorDriver) Abort() error {
	return fmt.Errorf("Not implemented")
}

// Join blocks the driver until it's either stopped or aborted.
func (e *MesosExecutorDriver) Join() error {
	return fmt.Errorf("Not implemented")
}

// Run runs the driver.
func (e *MesosExecutorDriver) Run() error {
	return fmt.Errorf("Not implemented")
}

// SendStatusUpdate send a StatusUpdate message to the slave.
func (e *MesosExecutorDriver) SendStatusUpdate() error {
	return fmt.Errorf("Not implemented")
}

// SendStatusUpdate send a FrameworkMessage to the slave.
func (e *MesosExecutorDriver) SendFrameworkMessage() error {
	return fmt.Errorf("Not implemented")
}
