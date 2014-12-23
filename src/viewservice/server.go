package viewservice

import (
	"container/list"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type ServerStat struct {
	DeadCount int
	alive     bool
	acked     uint
	idx       *list.Element
}

type Node struct {
	name  string
	alive bool
}

type ViewServer struct {
	mu         *sync.Mutex
	l          net.Listener
	dead       bool
	me         string
	view       View
	state      map[string]*ServerStat
	serverlist *list.List
	ack        uint
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()
	//first start
	if vs.view.Viewnum == 0 {
		vs.view.Primary = args.Me
		vs.view.Viewnum++
	}

	if args.Viewnum == 0 {
		server, ok := vs.state[args.Me]
		if ok {
			server.idx.Value = Node{args.Me, false}
		}
		idx := vs.serverlist.PushBack(Node{args.Me, true})
		vs.state[args.Me] = &ServerStat{0, true, 0, idx}

	} else {
		server, ok := vs.state[args.Me]
		if ok {
			server.DeadCount = 0
			if vs.isPrimary(args.Me) {
				vs.ack = vs.Max(vs.ack, args.Viewnum)
			}
		}
	}
	reply.View = vs.view

	vs.mu.Unlock()
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.mu.Lock()
	reply.View = vs.view
	vs.mu.Unlock()
	return nil
}

func (vs *ViewServer) isPrimary(name string) bool {
	return name == vs.view.Primary
}

func (vs *ViewServer) isBackup(name string) bool {
	return name == vs.view.Backup
}

func (vs ViewServer) hasPrimary() bool {
	return vs.view.Primary != ""
}

func (vs ViewServer) hasBackup() bool {
	return vs.view.Backup != ""
}

func (vs ViewServer) NotUsed(name string) bool {
	return name != vs.view.Primary && name != vs.view.Backup
}

func (vs ViewServer) Max(a, b uint) uint {
	if a >= b {
		return a
	}
	return b
}

func (vs *ViewServer) ChangeView() {
	changed := false

	if vs.view.Primary == "" && vs.view.Backup != "" {
		vs.view.Primary = vs.view.Backup
		vs.view.Backup = ""
		changed = true
	}

	for e := vs.serverlist.Front(); e != nil; e = e.Next() {
		name := e.Value.(Node).name
		//server := vs.state[name]
		//if vs.NotUsed(name) {
		if vs.view.Primary == "" {
			vs.view.Primary = name
			changed = true
		} else if vs.view.Backup == "" && name != vs.view.Primary {
			vs.view.Backup = name
			changed = true
			break
		}
		//}
	}

	if changed {
		vs.view.Viewnum++
	}
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	if vs.view.Viewnum == 0 {
		return
	}
	vs.mu.Lock()
	for e := vs.serverlist.Front(); e != nil; e = e.Next() {
		name := e.Value.(Node).name
		alive := e.Value.(Node).alive
		server := vs.state[name]

		if !alive {
			if name == vs.view.Primary {
				vs.view.Primary = ""
				vs.view.Backup = ""
			}
			vs.serverlist.Remove(e)
			continue
		}
		server.DeadCount++

		if server.DeadCount > DeadPings {
			server.alive = false
			vs.serverlist.Remove(e)
			delete(vs.state, name)
			//fmt.Printf("server delete  %s\n\n  acked = %d    current viewnum = %d \n\n", name, vs.ack, vs.view.Viewnum)
			if name == vs.view.Primary && vs.ack == vs.view.Viewnum {
				vs.view.Primary = ""
				vs.view.Backup = ""
			} else if name == vs.view.Backup {
				vs.view.Backup = ""
			}
		}
	}
	if !vs.hasPrimary() || !vs.hasBackup() {
		vs.ChangeView()
	}

	vs.mu.Unlock()
	//fmt.Println("--- vs me", vs.me)
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	vs.state = make(map[string]*ServerStat)
	vs.mu = &sync.Mutex{}
	vs.view = View{0, "", ""}
	vs.serverlist = list.New()
	vs.ack = 0
	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
