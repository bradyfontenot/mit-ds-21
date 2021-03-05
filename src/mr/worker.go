package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
)

// Map functions return a slice of KeyValue.

// KeyValue stores individual kv pairs
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker is called by main/mrworker.go
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	fmt.Print("From Worker")
	runMap(mapf)

	// request new file to map.

	// if no more files to map,
	// run reduce

	//*****************REDUCE************************
	// sort intermediate values
	// TODO: output and save into intermediate file to be ready by reduce
	//

	//
	// output files for reduce
	// oname := "mr-out-0"
	// ofile, _ := os.Create(oname)

	// //
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	// i := 0
	// for i < len(intermediate) {
	// 	j := i + 1
	// 	for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
	// 		j++
	// 	}
	// 	values := []string{}
	// 	for k := i; k < j; k++ {
	// 		values = append(values, intermediate[k].Value)
	// 	}
	// 	output := reducef(intermediate[i].Key, values)

	// 	// this is the correct format for each line of Reduce output.
	// 	fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

	// 	i = j
	// }

	// ofile.Close()

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// runMap handles logic process map files.
func runMap(mapf func(string, string) []KeyValue) {
	// make call to server for task
	// reply should contain filename
	response, err := requestMapFile()
	if err != nil {
		log.Fatal("request failed")
	}

	content := readInputFile(response.Filename)
	fmt.Print("CONTENT")
	kva := mapf(response.Filename, string(content))

	writeIntermediateFile(kva, response)

}

func readInputFile(filename string) string {
	// open input file,
	f, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	// read input file
	content, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	f.Close()

	return string(content)

}

func writeIntermediateFile(kva []KeyValue, response ResponseMsg) {
	// create hash partitions and store intermediate keys
	partitions := make(map[int][]KeyValue)
	for _, kv := range kva {
		kv.Key = strings.ToLower(kv.Key)
		rTask := ihash(kv.Key) % response.NReduce
		partitions[rTask] = append(partitions[rTask], kv)
	}

	// json encode and write buckets to intermediate files
	for i, p := range partitions {
		sort.Sort(ByKey(p))
		// create file
		fName := "mr-" + strconv.Itoa(response.ID) + "-" + strconv.Itoa(i)
		fFile, err := os.Create(fName)
		if err != nil {
			log.Fatalf("Could not create intermediate file: %v", err)
		}
		// create encoder
		enc := json.NewEncoder(fFile)
		// write kv pairs to file in json format
		for _, kv := range p {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("write to intermediate file failed: %v", err)
			}
		}
		fFile.Close()
	}
}

// RequestFile asks coordinator for a file to process
func requestMapFile() (ResponseMsg, error) {
	var args RequestMsg
	reply := ResponseMsg{}

	call("Coordinator.SendMapFile", &args, &reply)

	return reply, nil
}

func requestReduceFile() (ResponseMsg, error) {
	var args interface{}
	reply := ResponseMsg{}

	call("Coordinator.SendReduceFile", &args, &reply)

	return reply, nil
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	cli, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer cli.Close()

	err = cli.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
