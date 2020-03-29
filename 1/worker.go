package mr

import "bufio"
import "strings"
import "os"
import "io"
import "fmt"
import "log"
import "net/rpc"
import "strconv"
import "hash/fnv"
import "io/ioutil"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	//var filename string
	args := ExampleArgs{}
	reply := ExampleReply{}
	CallforTask(&args, &reply)
	if reply.N >= 0 {
		var prefix string = "mr-"+strconv.Itoa(reply.N)+"-"
		for i:=0;i<10;i++{
			os.Remove(prefix+strconv.Itoa(i))
			oname := prefix+strconv.Itoa(i)
			ofile, _ := os.Create(oname)
			ofile.Close()
		}
		//map part
		file, err := os.Open(reply.Y)
		if err != nil {
			log.Fatalf("cannot open %v", reply.Y)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", reply.Y)
		}
		file.Close()
		kva := mapf(reply.Y, string(content))
		
		for _,v := range kva {
			
			var suffix int = ihash(v.Key)%10
			ofile,err:=os.OpenFile(prefix+strconv.Itoa(suffix),os.O_RDWR|os.O_CREATE|os.O_APPEND,0666)
			if err != nil {
				log.Fatalf("cannot read %v", prefix+strconv.Itoa(suffix))
			}
			fmt.Fprintf(ofile, "%v %v\n", v.Key, v.Value)
			
			ofile.Close()
		}
	} else if reply.N!=-100 {
		fmt.Println("------------")
		//reduce part
		var m map[string]int
		m = make(map[string]int)
		fmt.Println(reply.totalMap)
		for i:=0;i<8;i++{
			filename := "mr-"+strconv.Itoa(i)+"-"+strconv.Itoa(-reply.N)
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			br := bufio.NewReader(file)
			for {
				a, _, c := br.ReadLine()
				if c == io.EOF {
					break
				}
				b := string(a)
				//fmt.Println("a111")
				idx := strings.Index(b, " ")
				m[b[:idx]]++
			}

			file.Close()
		}

		oname := "mr-out-"+strconv.Itoa(-reply.N)
		ofile, _ := os.Create(oname)
		for key, value := range m {
			fmt.Fprintf(ofile, "%v %v\n", key, strconv.Itoa(value))
		}
		ofile.Close()
		args.X=1
		CallforTask(&args, &reply)

	}
	//fmt.Println(filename)
	// uncomment to send the Example RPC to the master.
	//CallExample()

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallforTask(args *ExampleArgs, reply *ExampleReply) {

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
