package mr

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.
type RequestMsg struct {
}

type ResponseMsg struct {
	Filename string
	ID       int
	NReduce  int
}

type FileStatus struct {
	Filename string
	Status   string
}

type Receipt struct {
	Success bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
