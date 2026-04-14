package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type LockState string

const (
	STATE_LOCKED   = "STATE_LOCKED"
	STATE_UNLOCKED = "STATE_UNLOCKED"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk

	// You may add code here
	lockId string
	curVer rpc.Tversion
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:     ck,
		lockId: l,
		curVer: 0,
	}
	// You may add code here
	return lk
}

// [ral6h]: from the exercise description:
//
// If a client crashes while holding a lock, the lock will never be released.
// In a design more sophisticated than this lab, the client would attach a lease to a lock.
// When the lease expires, the lock server would release the lock on behalf of the client.
// In this lab clients don't crash and you can ignore this problem.
func (lk *Lock) Acquire() {
	/*
		loop:
			get l -> state, version
			if errNoKey try insert (we expect curVer = 0 at this point right)

			update curVer if it differs

			if state == locked continue loop
			else try put with state = locked and our version + 1
			if errVersion we lost the race -> continue
			else we now own the lock with our version and we break the loop happily :D
	*/

acquire_loop:
	for {
		state, version, err := lk.ck.Get(lk.lockId)

		switch err {
		case rpc.ErrNoKey:
			if lk.curVer != 0 {
				panic("Got ErrNoKey but current version > 0")
			}
			putErr := lk.ck.Put(lk.lockId, STATE_LOCKED, lk.curVer)

			switch putErr {
			case rpc.OK:
				//we currently own the lock yay
				lk.curVer += 1
				break acquire_loop
			case rpc.ErrVersion:
				//we lost the race and someoneelse got to create the lock b4 us
				continue acquire_loop
			case rpc.ErrMaybe: //TODO: handle correctly, should not be thrown in a reliable net
				continue acquire_loop
			}

		case rpc.OK:
			if lk.curVer != version {
				lk.curVer = version
			}

			switch state {
			case STATE_LOCKED:
				continue acquire_loop
			case STATE_UNLOCKED:
				putErr := lk.ck.Put(lk.lockId, STATE_LOCKED, lk.curVer)

				switch putErr {
				case rpc.OK:
					//we currently own the lock yay
					lk.curVer += 1
					break acquire_loop
				case rpc.ErrVersion:
					//we lost the race and someoneelse got to create the lock b4 us
					continue acquire_loop
				case rpc.ErrMaybe: //TODO: handle correctly, should not be thrown in a reliable net
					continue acquire_loop
				}
			}
		}
	}
}

func (lk *Lock) Release() {
	err := lk.ck.Put(lk.lockId, STATE_UNLOCKED, lk.curVer)

	if err != rpc.OK {
		panic("Calling release on a non owned lock")
	}

	lk.curVer += 1
}
