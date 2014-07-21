package main

import (
	"flag"
	"fmt"

	"github.com/yifan-gu/go-mesos/executor"
	"github.com/yifan-gu/go-mesos/mesosproto"
)

type ExampleExecutor struct {
	done chan struct{}
}

func (exec *ExampleExecutor) Registered(executor.ExecutorDriver, *mesosproto.ExecutorInfo, *mesosproto.FrameworkInfo, *mesosproto.SlaveInfo) {
	fmt.Println("Executor Registered")
}

func (exec *ExampleExecutor) Reregistered(executor.ExecutorDriver, *mesosproto.SlaveInfo) {
	fmt.Println("Executor Reregistered")
}

func (exec *ExampleExecutor) Disconnected(executor.ExecutorDriver) {
	fmt.Println("Executor disconnected")
}

func (exec *ExampleExecutor) LaunchTask(driver executor.ExecutorDriver, taskInfo *mesosproto.TaskInfo) {
	fmt.Println("Launching task", taskInfo.GetName())
	st, err := driver.SendStatusUpdate()
	if err != nil {
		fmt.Println("Got error", err)
	}
	go func() {
		time.Sleep(time.Second * 3)
		fmt.Println("Task finished", taskInfo.GetName())
		st, err := driver.SendStatusUpdate()
		if err != nil {
			fmt.Println("Got error", err)
		}
	}()
}

func (exec *ExampleExecutor) KillTask(executor.ExecutorDriver, *mesosproto.TaskID) {
	fmt.Println("Kill task")
}

func (exec *ExampleExecutor) FrameworkMessage(executor.ExecutorDriver, string) {
	fmt.Println("Got framework message")
}

func (exec *ExampleExecutor) Shutdown(executor.ExecutorDriver) {
	fmt.Println("Shutting down the executor")
	close(exec.done)
}

func (exec *ExampleExecutor) Error(executor.ExecutorDriver, string) {
	fmt.Println("Got error message")
}

func main() {
	flag.Parse()
	driver := executor.NewMesosExecutorDriver()
	driver.Executor = &ExampleExecutor{done: make(chan struct{})}
	status, err := driver.Start()
	if err != nil {
		fmt.Println("Got error:", err)
		return
	}
	<-exec.done
}
