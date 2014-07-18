package main

import (
	"fmt"

	"github.com/yifan-gu/go-mesos/executor"
	"github.com/yifan-gu/go-mesos/mesosproto"
)

type ExampleExecutor bool

func (exec *ExampleExecutor) Registered(executor.ExecutorDriver, *mesosproto.ExecutorInfo, *mesosproto.FrameworkInfo, *mesosproto.SlaveInfo) {
	fmt.Println("Registered!!!")
}
func (exec *ExampleExecutor) Reregistered(executor.ExecutorDriver, *mesosproto.SlaveInfo) {}
func (exec *ExampleExecutor) Disconnected(executor.ExecutorDriver)                        {}
func (exec *ExampleExecutor) LaunchTask(executor.ExecutorDriver, *mesosproto.TaskInfo)    {}
func (exec *ExampleExecutor) KillTask(executor.ExecutorDriver, *mesosproto.TaskID)        {}
func (exec *ExampleExecutor) FrameworkMessage(executor.ExecutorDriver, string)            {}
func (exec *ExampleExecutor) Shutdown(executor.ExecutorDriver)                            {}
func (exec *ExampleExecutor) Error(executor.ExecutorDriver, string)                       {}

func main() {
	driver := executor.NewMesosExecutorDriver()
	driver.Executor = new(ExampleExecutor)
	status, err := driver.Start()
	if err != nil {
		fmt.Println("Get error:", err)
		return
	}
	fmt.Println(status)
	select {}
}
