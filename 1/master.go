package mr

import "log"
import "fmt"
import "net"
import "os"
import "net/rpc"
import "net/http"


type Master struct {
	filename []string
	total int
	cur_m int
	reducer int
	cur_r int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	if args.X==1 {
		m.reducer--
		return nil
	}
	if m.cur_r>10 {
		reply.N = -100
		return nil
	}
	if m.cur_m==m.total {
		reply.totalMap = m.total
		fmt.Println(m.total)
		fmt.Println(reply.totalMap)
		reply.Y = ""
		reply.N = -m.cur_r
		m.cur_r++
	} else {
		reply.Y = m.filename[m.cur_m]
		reply.N = m.cur_m
		m.cur_m++
	}
	
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	if m.reducer<=0 {
		ret = true
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.filename = files
	m.total = len(files)
	m.cur_m = 0
	m.reducer = 10
	m.cur_r = 1

	m.server()
	return &m
}
