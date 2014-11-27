package mapreduce

import "container/list"
import "fmt"
import "math"

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
	book := make(map[int]*WorkerInfo)

	idx := 0
	for _, w := range mr.Workers {
		if idx > (mr.nWorker - 1) {
			break
		}
		for i := 0; i < int(math.Floor(float64(mr.nMap/mr.nWorker))); i++ {
			go func(idx int, i int) {
				JobNumber := idx + i*mr.nWorker
				args := &DoJobArgs{mr.file, "Map", JobNumber, mr.nReduce}
				var reply = &DoJobReply{}
				book[JobNumber] = w
				ok := call(w.address, "Worker.DoJob", args, reply)
				if ok == true {
					mr.JobDoneChannel <- args.JobNumber
				}
			}(idx, i)
		}
		idx++
	}

	for i := 0; i < mr.nMap; i++ {
		<-mr.JobDoneChannel
	}
	for i := 0; i < mr.nReduce; i++ {
		go func(JobNumber int) {
			args := &DoJobArgs{mr.file, "Reduce", JobNumber, mr.nMap}
			var reply = &DoJobReply{}
			w := book[JobNumber]
			ok := call(w.address, "Worker.DoJob", args, reply)
			if ok == true {
				mr.JobDoneChannel <- args.JobNumber
			}
		}(i)
	}
	for i := 0; i < mr.nReduce; i++ {
		<-mr.JobDoneChannel
	}

	return mr.KillWorkers()
}
