package viewservice

import (
	"container/list"
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type ServerStat struct {
	LastPing  time.Time
	DeadCount int
	alive     bool
	acked     bool
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
			delete(vs.state, args.Me)
		}
		vs.serverlist.PushBack(Node{args.Me, true})
		vs.state[args.Me] = &ServerStat{time.Now(), 0, true, false, vs.serverlist.Back()}
	} else {
		server, ok := vs.state[args.Me]
		if !ok {
			return errors.New("no such server or view.")
		}
		server.LastPing = time.Now()
		server.acked = true
	}
	reply.View = vs.view

	vs.mu.Unlock()
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	reply.View = vs.view
	return nil
}

func (vs ViewServer) NotUsed(name string) bool {
	return name != vs.view.Primary && name != vs.view.Backup
}

func (vs *ViewServer) ChangeView() {
	changed := false
	//if vs.state[vs.view.Primary].acked {
	for e := vs.serverlist.Front(); e != nil; e = e.Next() {
		name := e.Value.(Node).name
		server := vs.state[name]
		if vs.NotUsed(name) && server.alive {
			if vs.view.Primary == "" {
				vs.view.Primary = name
				changed = true
			} else if vs.view.Backup == "" {
				vs.view.Backup = name
				changed = true
				break
			}
		}
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
	//for name, server := range vs.state {
	//for idx, name := range vs.serverlist {
	for e := vs.serverlist.Front(); e != nil; e = e.Next() {
		name := e.Value.(Node).name
		server := vs.state[name]

		if !e.Value.(Node).alive {
			if name == vs.view.Primary {
				vs.view.Primary = ""
				vs.view.Backup = ""
			}
			//delete(vs.state, name)
			vs.serverlist.Remove(e)
			continue
		}

		if time.Since(server.LastPing) > PingInterval {
			server.DeadCount++
		}
		if server.DeadCount >= DeadPings {
			server.alive = false
			vs.serverlist.Remove(e)
			delete(vs.state, name)
			if name == vs.view.Primary {
				vs.view.Primary = ""
				vs.view.Backup = ""
			} else if name == vs.view.Backup {
				vs.view.Backup = ""
			}
			//vs.IdleServers = append(vs.ServerList[:server.idx], vs.ServersList[server.idx+1:]...)
		}
	}
	if vs.view.Primary == "" || vs.view.Backup == "" {
		vs.ChangeView()
	}

	vs.mu.Unlock()
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
	//vs.serverlist = make([]string, 10)
	vs.serverlist = list.New()
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
