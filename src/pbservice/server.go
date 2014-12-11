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

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type PBServer struct {
	l          net.Listener
	dead       bool // for testing
	unreliable bool // for testing
	me         string
	vs         *viewservice.Clerk
	done       sync.WaitGroup
	finish     chan interface{}
	view       viewservice.View
	whoami     string
	db         map[string]string
	mu         *sync.Mutex
	filter     map[string]string

	// Your declarations here.
}

func (pb *PBServer) SetWhoAmI(view viewservice.View) {
	pb.mu.Lock()
	if pb.me == view.Primary {
		pb.whoami = "Primary"
	} else if pb.me == view.Backup {
		pb.whoami = "Backup"
	} else {
		pb.whoami = "Unknown"
	}
	pb.mu.Unlock()
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	var Value string
	pb.mu.Lock()
	token := hash(strconv.Itoa(int(args.Token)) + args.me)
	previous, ok := pb.filter[args.me]
	if ok {
		if previous == token {
			//reject dupicate
			pb.mu.Unlock()
			return nil
		}
	} else {
		pb.filter[args.me] = token
	}

	if args.DoHash {
		v, ok := pb.db[args.Key]
		if !ok {
			v = ""
		}
		reply.PreviousValue = v
		Value = strconv.Itoa(int(hash(v + args.Value)))
	} else {
		Value = args.Value
	}
	pb.db[args.Key] = Value

	//Forwards the updates to the backcup
	var BackupReply PutReply
	args.Token = nrand()
	args.Me = pb.me
	for !call(pb.view.Backup, "PBServer.Put", args, &BackupReply) {
	}

	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	if args.GetAll {
		reply.db = pb.db
	} else {
		v, ok := pb.db[args.Key]
		if !ok {
			reply.Err = "no such key/value."
			v = ""
			//return errors.New("no such key/value")
		}
		reply.Value = v
	}
	pb.mu.Unlock()
	return nil
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
	view, _ := pb.vs.Ping(pb.view.Viewnum)
	pb.SetWhoAmI(view)
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
	pb.filter = make(map[string]string)
	pb.mu = &sync.Mutex{}

	pb.view, _ = pb.vs.Ping(0)

	if pb.view.Backup == pb.me {
		args := &GetArgs{"", true}
		var reply GetReply

		for !call(pb.view.Primary, "PBServer.Get", args, &reply) {
		}
		pb.db = reply.db
	}

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
