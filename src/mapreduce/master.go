package mapreduce

import "container/list"
import "fmt"

//import "math"

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	mr.JobDoneChannel = make(chan int)

	for i := 0; i < mr.nMap; i++ {
		go func(JobNumber int) {
			for {
				w := <-mr.idleChannel
				args := &DoJobArgs{mr.file, "Map", JobNumber, mr.nReduce}
				var reply = &DoJobReply{}
				ok := call(w, "Worker.DoJob", args, reply)
				if ok == true {
					mr.JobDoneChannel <- args.JobNumber
					mr.idleChannel <- w
					return
				}
			}
		}(i)
	}

	for i := 0; i < mr.nMap; i++ {
		<-mr.JobDoneChannel
	}

	for i := 0; i < mr.nReduce; i++ {
		go func(JobNumber int) {
			for {
				w := <-mr.idleChannel
				args := &DoJobArgs{mr.file, "Reduce", JobNumber, mr.nMap}
				var reply = &DoJobReply{}
				ok := call(w, "Worker.DoJob", args, reply)
				if ok == true {
					mr.JobDoneChannel <- args.JobNumber
					mr.idleChannel <- w
					return
				}
			}
		}(i)
	}

	for i := 0; i < mr.nReduce; i++ {
		<-mr.JobDoneChannel
	}

	return mr.KillWorkers()
}
