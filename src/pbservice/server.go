package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"
import "strconv"
import "errors"

//import "errors"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type Node struct {
	Token         uint32
	PreviousValue string
}

type PBServer struct {
	l            net.Listener
	dead         bool // for testing
	unreliable   bool // for testing
	me           string
	vs           *viewservice.Clerk
	done         sync.WaitGroup
	finish       chan interface{}
	view         viewservice.View
	whoami       string
	db           map[string]string
	mu           *sync.Mutex
	filter       map[string]Node
	initialnized bool

	// Your declarations here.
}

func (pb *PBServer) SetWhoAmI(view viewservice.View) {
	if pb.me == view.Primary {
		pb.whoami = "Primary"
	} else if pb.me == view.Backup {
		pb.whoami = "Backup"
	} else {
		pb.whoami = "Unknown"
	}
}

func (pb *PBServer) UpdateServer() {
	pb.view, _ = pb.vs.Ping(pb.view.Viewnum)
	pb.SetWhoAmI(pb.view)
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	var Value string
	token := hash(strconv.Itoa(int(args.Token)) + args.Me)
	pb.mu.Lock()

	if pb.whoami != "Primary" {
		//pb.UpdateServer()
		reply.Err = ErrWrongServer
		pb.mu.Unlock()
		return errors.New("[Put]Not Primary.Error server.")
	}

	previous, ok := pb.filter[args.Me]
	if ok && previous.Token == token {
		//reject dupicate
		reply.PreviousValue = previous.PreviousValue
		pb.mu.Unlock()
		return nil
	}

	val, ok := pb.db[args.Key]
	if !ok {
		val = ""
	}
	reply.PreviousValue = val

	if args.DoHash {
		//fmt.Printf("previous  %v  token   %v current val %v      previous val %v \n", previous, token, args.Value, val)
		Value = strconv.Itoa(int(hash(val + args.Value)))
	} else {
		Value = args.Value
	}

	//Forwards the updates to the backcup
	if pb.view.Backup != "" {
		var BackupReply PutReply
		args.Token = nrand()
		args.Me = pb.me
		ok := call(pb.view.Backup, "PBServer.SyncPut", args, &BackupReply)
		if !ok {
			//time.Sleep(viewservice.PingInterval)
			//pb.UpdateServer()
			fmt.Println(pb.view)
			pb.mu.Unlock()
			return errors.New("Sync Fail.")
		}
		// cnt := 0
		// for !call(pb.view.Backup, "PBServer.SyncPut", args, &BackupReply) {
		// 	if cnt >= RETRY {
		// 		pb.view, _ = pb.vs.Ping(pb.view.Viewnum)
		// 		pb.SetWhoAmI(pb.view)
		// 		cnt = 0
		// 		if pb.view.Backup == "" {
		// 			break
		// 		}
		// 	} else {
		// 		cnt++
		// 	}
		// 	time.Sleep(viewservice.PingInterval)
		// }
	}
	pb.filter[args.Me] = Node{token, val}
	pb.db[args.Key] = Value
	pb.mu.Unlock()

	return nil
}

func (pb *PBServer) SyncPut(args *PutArgs, reply *PutReply) error {
	var Value string
	pb.mu.Lock()
	if pb.whoami != "Backup" {
		//pb.UpdateServer()
		reply.Err = ErrWrongServer
		pb.mu.Unlock()
		return errors.New("[SyncPut]Not Backup.Error server.")
	}
	token := hash(strconv.Itoa(int(args.Token)) + args.Me)
	defer pb.mu.Unlock()

	previous, ok := pb.filter[args.Me]
	if ok && previous.Token == token {
		reply.PreviousValue = previous.PreviousValue
		return nil
	}

	if args.DoHash {
		val, ok := pb.db[args.Key]
		if !ok {
			val = ""
		}
		reply.PreviousValue = val
		pb.filter[args.Me] = Node{token, val}
		Value = strconv.Itoa(int(hash(val + args.Value)))
	} else {
		Value = args.Value
	}
	pb.db[args.Key] = Value
	return nil

}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.whoami != "Primary" {
		//pb.UpdateServer()
		reply.Err = ErrWrongServer
		return errors.New("[Get]Not Primary.Error server.")
	}

	if args.GetAll {
		reply.Db = pb.db
		if pb.view.Backup == "" {
			pb.UpdateServer()
		}
	} else {
		v, ok := pb.db[args.Key]
		if !ok {
			reply.Err = ErrNoKey
			v = ""
			return errors.New("no such key/value")
		}
		reply.Value = v
	}
	return nil
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
	pb.mu.Lock()
	pb.UpdateServer()

	if !pb.initialnized && pb.view.Backup == pb.me && pb.view.Primary != "" {

		args := &GetArgs{"", true}
		var reply GetReply
		reply.Db = make(map[string]string)

		ok := call(pb.view.Primary, "PBServer.Get", args, &reply)
		// for !call(pb.view.Primary, "PBServer.Get", args, &reply) {
		// 	time.Sleep(viewservice.PingInterval)
		// }
		if ok {
			for k, v := range reply.Db {
				pb.db[k] = v
			}
			pb.initialnized = true
		}
	}

	pb.mu.Unlock()
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.finish = make(chan interface{})
	pb.whoami = "Unknown"
	pb.db = make(map[string]string)
	pb.filter = make(map[string]Node)
	pb.mu = &sync.Mutex{}
	pb.initialnized = false

	pb.view, _ = pb.vs.Ping(0)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				} else {
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", pb.me)
		pb.done.Wait()
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(pb.finish)
	}()

	pb.done.Add(1)
	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
		pb.done.Done()
	}()

	return pb
}
