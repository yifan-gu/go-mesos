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
	"os"
	"sync"
	"time"

	"code.google.com/p/go-uuid/uuid"
	"code.google.com/p/gogoprotobuf/proto"
	log "github.com/golang/glog"
	"github.com/yifan-gu/go-mesos/mesosproto"
	"github.com/yifan-gu/go-mesos/messenger"
	"github.com/yifan-gu/go-mesos/upid"
)

const (
	defaultRecoveryTimeout = time.Minute * 15
	// MesosVersion indicates the supported mesos version.
	MesosVersion = "0.20.0"
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
	Start() (mesosproto.Status, error)
	Stop() (mesosproto.Status, error)
	Abort() (mesosproto.Status, error)
	Join() (mesosproto.Status, error)
	SendStatusUpdate(*mesosproto.TaskStatus) (mesosproto.Status, error)
	SendFrameworkMessage(string) (mesosproto.Status, error)
	Destroy() error
}

// MesosExecutorDriver is a implementation of the ExecutorDriver.
type MesosExecutorDriver struct {
	Executor        Executor
	mutex           sync.Mutex
	status          mesosproto.Status
	messenger       messenger.Messenger
	slave           *upid.UPID
	frameworkID     *mesosproto.FrameworkID
	executorID      *mesosproto.ExecutorID
	workDir         string
	connected       bool
	connection      uuid.UUID
	local           bool
	aborted         bool
	cond            chan bool
	directory       string
	checkpoint      bool
	recoveryTimeout time.Duration
	updates         map[string]*mesosproto.StatusUpdate // Key is a UUID string.
	tasks           map[string]*mesosproto.TaskInfo     // Key is a UUID string.
}

// NewMesosExecutorDriver creates a new mesos executor driver.
func NewMesosExecutorDriver() *MesosExecutorDriver {
	driver := &MesosExecutorDriver{}

	driver.cond = make(chan bool, 1)
	driver.updates = make(map[string]*mesosproto.StatusUpdate)
	driver.tasks = make(map[string]*mesosproto.TaskInfo)
	// TODO(yifan): Set executor cnt.
	driver.messenger = messenger.NewMesosMessenger(&upid.UPID{ID: "Executor(1)"})
	return driver
}

// init initializes the driver.
func (driver *MesosExecutorDriver) init() error {
	log.Infof("Init mesos executor driver\n Version: %v\n", MesosVersion)

	// Install handlers.
	// TODO(yifan): Check errors.
	driver.messenger.Install(driver.registered, &mesosproto.ExecutorRegisteredMessage{})
	driver.messenger.Install(driver.reregistered, &mesosproto.ExecutorReregisteredMessage{})
	driver.messenger.Install(driver.reconnect, &mesosproto.ReconnectExecutorMessage{})
	driver.messenger.Install(driver.runTask, &mesosproto.RunTaskMessage{})
	driver.messenger.Install(driver.killTask, &mesosproto.KillTaskMessage{})
	driver.messenger.Install(driver.statusUpdateAcknowledgement, &mesosproto.StatusUpdateAcknowledgementMessage{})
	driver.messenger.Install(driver.frameworkMessage, &mesosproto.FrameworkToExecutorMessage{})
	driver.messenger.Install(driver.shutdown, &mesosproto.ShutdownExecutorMessage{})
	return nil
}

// Start starts the driver.
func (driver *MesosExecutorDriver) Start() (mesosproto.Status, error) {
	log.Infoln("Start mesos executor driver")

	driver.mutex.Lock()
	defer driver.mutex.Unlock()

	if driver.status != mesosproto.Status_DRIVER_NOT_STARTED {
		return driver.status, nil
	}
	if err := driver.parseEnviroments(); err != nil {
		log.Errorf("Failed to parse environments: %v\n", err)
		return mesosproto.Status_DRIVER_NOT_STARTED, err
	}
	if err := driver.init(); err != nil {
		log.Errorf("Failed to initialize the driver: %v\n", err)
		return mesosproto.Status_DRIVER_NOT_STARTED, err
	}

	// Start the messenger.
	if err := driver.messenger.Start(); err != nil {
		log.Errorf("Failed to start the messenger: %v\n", err)
		return mesosproto.Status_DRIVER_NOT_STARTED, err
	}

	// Register with slave.
	message := &mesosproto.RegisterExecutorMessage{
		FrameworkId: driver.frameworkID,
		ExecutorId:  driver.executorID,
	}
	driver.messenger.Send(driver.slave, message)

	// Set status.
	driver.status = mesosproto.Status_DRIVER_RUNNING
	return driver.status, nil
}

// Stop stops the driver.
func (driver *MesosExecutorDriver) Stop() (mesosproto.Status, error) {
	return mesosproto.Status_DRIVER_NOT_STARTED, fmt.Errorf("Not implemented")
}

// Abort aborts the driver.
func (driver *MesosExecutorDriver) Abort() (mesosproto.Status, error) {
	return mesosproto.Status_DRIVER_NOT_STARTED, fmt.Errorf("Not implemented")
}

// Join blocks the driver until it's either stopped or aborted.
func (driver *MesosExecutorDriver) Join() (mesosproto.Status, error) {
	return mesosproto.Status_DRIVER_NOT_STARTED, fmt.Errorf("Not implemented")
}

// SendStatusUpdate sends a StatusUpdate message to the slave.
func (driver *MesosExecutorDriver) SendStatusUpdate(*mesosproto.TaskStatus) (mesosproto.Status, error) {
	return mesosproto.Status_DRIVER_NOT_STARTED, fmt.Errorf("Not implemented")
}

// SendFrameworkMessage sends a FrameworkMessage to the slave.
func (driver *MesosExecutorDriver) SendFrameworkMessage(string) (mesosproto.Status, error) {
	return mesosproto.Status_DRIVER_NOT_STARTED, fmt.Errorf("Not implemented")
}

// Destroy destroys the driver.
func (driver *MesosExecutorDriver) Destroy() error {
	return fmt.Errorf("Not implemented")
}

func (driver *MesosExecutorDriver) parseEnviroments() error {
	var value string

	value = os.Getenv("MESOS_LOCAL")
	if len(value) > 0 {
		driver.local = true
	}

	value = os.Getenv("MESOS_SLAVE_PID")
	if len(value) == 0 {
		err := fmt.Errorf("Cannot find MESOS_SLAVE_PID in the environment")
		log.Errorf("Failed to parse environments: %v\n", err)
		return err
	}
	upid, err := upid.Parse(value)
	if err != nil {
		log.Errorf("Cannot parse UPID %v\n", err)
		return err
	}
	driver.slave = upid

	value = os.Getenv("MESOS_FRAMEWORK_ID")
	// TODO(yifan): Check if the value exists?
	driver.frameworkID = new(mesosproto.FrameworkID)
	driver.frameworkID.Value = proto.String(value)

	value = os.Getenv("MESOS_EXECUTOR_ID")
	// TODO(yifan): Check if the value exists?
	driver.executorID = new(mesosproto.ExecutorID)
	driver.executorID.Value = proto.String(value)

	value = os.Getenv("MESOS_DIRECTORY")
	// TODO(yifan): Check if the value exists?
	driver.workDir = value

	value = os.Getenv("MESOS_CHECKPOINT")
	if value == "1" {
		driver.checkpoint = true
	}
	// TODO(yifan): Parse the duration. For now just use default.
	return nil
}

func (driver *MesosExecutorDriver) registered(pbMsg proto.Message) {
	msg := pbMsg.(*mesosproto.ExecutorRegisteredMessage)
	driver.Executor.Registered(driver, msg.GetExecutorInfo(), msg.GetFrameworkInfo(), msg.GetSlaveInfo())
}

func (driver *MesosExecutorDriver) reregistered(pbMsg proto.Message) {
	panic("Not implemented")
}

func (driver *MesosExecutorDriver) reconnect(pbMsg proto.Message) {
	panic("Not implemented")
}

func (driver *MesosExecutorDriver) runTask(pbMsg proto.Message) {
	panic("Not implemented")
}

func (driver *MesosExecutorDriver) killTask(pbMsg proto.Message) {
	panic("Not implemented")
}

func (driver *MesosExecutorDriver) statusUpdateAcknowledgement(pbMsg proto.Message) {
	panic("Not implemented")
}

func (driver *MesosExecutorDriver) frameworkMessage(pbMsg proto.Message) {
	panic("Not implemented")
}

func (driver *MesosExecutorDriver) shutdown(pbMsg proto.Message) {
	panic("Not implemented")
}
