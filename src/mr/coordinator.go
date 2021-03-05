package mr

import (
	"container/list"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type fileStatus string

const (
	queued  fileStatus = "queued"
	running fileStatus = "running"
	done    fileStatus = "done"
)

type File struct {
	Status fileStatus
	ID     int
}

// Your definitions here.
type Coordinator struct {
	mapFiles    map[string]*File
	mapQueue    list.List
	reduceQueue list.List
	reduceFiles map[string]*File
	nReduce     int
	sync.RWMutex
}

// MakeCoordinator creates a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	fmt.Print("OOPS")
	c := &Coordinator{
		mapFiles:    make(map[string]*File),
		reduceFiles: make(map[string]*File),
		nReduce:     nReduce,
	}

	// put filenames in a queue
	for i, f := range files {
		c.mapFiles[f] = &File{
			Status: queued,
			ID:     i,
		}
		c.mapQueue.PushBack(f)
	}
	fmt.Printf("%+v", c)
	c.server()
	return c
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// RPC handlers for the worker to call.

// SendMapFile is an RPC service method
func (c *Coordinator) SendMapFile(args *RequestMsg, reply *ResponseMsg) error {
	c.Lock()
	defer c.Unlock()

	// check if queue is empty

	// grab next file from queue
	file := c.mapQueue.Front()
	c.mapQueue.Remove(file)
	filename, ok := file.Value.(string)
	if !ok {
		*reply = ResponseMsg{
			Filename: "",
		}
		return errors.New("could not assert type on element in file queue")
	}
	// update file status
	c.mapFiles[filename].Status = running

	*reply = ResponseMsg{
		Filename: filename,
		ID:       c.mapFiles[filename].ID,
		NReduce:  c.nReduce,
	}
	fmt.Printf("%v", *reply)
	// go c.healthcheck(filename)

	return nil
}

// SendReduceFile is an RPC service method
func (c *Coordinator) SendReduceFile(args interface{}, reply *ResponseMsg) error {

	return nil
}

// UpdateFileStatus is an RPC service method
func (c *Coordinator) UpdateFileStatus(args *FileStatus, reply *Receipt) {

	reply.Success = true
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

func (c *Coordinator) healthcheck(filename string) {
	time.Sleep(10 * time.Second)
	c.Lock()
	defer c.Unlock()

	if c.mapFiles[filename].Status != done {
		c.mapQueue.PushBack(filename)
	}
}
