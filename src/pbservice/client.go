package pbservice

import "viewservice"
import "sync"
import "time"
import "strconv"

//import "fmt"

// You'll probably need to uncomment these:
// import "time"
// import "crypto/rand"
// import "math/big"

type Clerk struct {
	vs     *viewservice.Clerk
	Me     string
	server string
	mu     *sync.Mutex
	view   viewservice.View
	vshost string
	// Your declarations here
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	ck.vshost = vshost
	//ck.Me = me
	ck.Me = strconv.FormatInt(nrand(), 10)

	ck.mu = &sync.Mutex{}

	view, _ := ck.vs.Get()
	ck.view = view
	ck.server = ck.view.Primary

	return ck
}

func (ck *Clerk) UpdateServer() {
	//ck.mu.Lock()
	view, ok := ck.vs.Get()
	if !ok {
		//fmt.Println("********** vs", ck.vshost)

		return
	}
	ck.view = view
	ck.server = ck.view.Primary
	//fmt.Println("^^^ client view ", ck.view)
	//ck.mu.Unlock()
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{key, false}
	var reply GetReply
	cnt := 0

	//if ck.view.Viewnum == 0 {
	if ck.server == "" {
		ck.UpdateServer()
	}

	for !call(ck.server, "PBServer.Get", args, &reply) {
		time.Sleep(viewservice.PingInterval)
		//fmt.Println("-----------------  get" + ck.Me)
		if reply.Err == ErrWrongServer || cnt >= RETRY {
			//fmt.Println(ck.view)
			ck.UpdateServer()
			cnt = 0
		} else {
			cnt++
		}
	}

	return reply.Value
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
	args := &PutArgs{key, value, dohash, nrand(), ck.Me}
	var reply PutReply
	cnt := 0

	if ck.server == "" {
		ck.UpdateServer()
	}
	for !call(ck.server, "PBServer.Put", args, &reply) {
		time.Sleep(viewservice.PingInterval)
		//fmt.Println("-----------------  put" + ck.Me)
		if reply.Err == ErrWrongServer || cnt >= RETRY {
			ck.UpdateServer()
			cnt = 0
		} else {
			cnt++
		}
	}

	return reply.PreviousValue
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
	v := ck.PutExt(key, value, true)
	return v
}
