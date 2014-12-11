package pbservice

import "viewservice"
import "fmt"

// You'll probably need to uncomment these:
// import "time"
// import "crypto/rand"
// import "math/big"

type Clerk struct {
	vs     *viewservice.Clerk
	Me     string
	server string
	// Your declarations here
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	ck.Me = me

	view, _ := ck.vs.Ping(0)
	ck.server = view.Primary

	return ck
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

	for !call(ck.server, "PBServer.Get", args, &reply) {
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

	for !call(ck.server, "PBServer.Put", args, &reply) {
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
