package keyring

type KeyRing interface {
	Checkout(string) bool
	Return(string)
}

type checkoutRequest struct {
	key          string
	responseChan chan bool
}

type mapKeyRing struct {
	Ring     map[string]bool
	checkout chan *checkoutRequest
	ret      chan string
}

func New() KeyRing {
	kr := &mapKeyRing{
		Ring:     make(map[string]bool),
		checkout: make(chan *checkoutRequest),
		ret:      make(chan string),
	}
	go kr.Crank()
	return kr
}

func (kr *mapKeyRing) Crank() {
	for {
		select {
		case req := <-kr.checkout:
			checkedout, ok := kr.Ring[req.key]
			if !ok { // Initialize the key to be not checked out.
				checkedout = false
				kr.Ring[req.key] = checkedout
			}
			if !checkedout { // Then if its not checkedout check the key out.
				kr.Ring[req.key] = true
			}
			// Return the status of the key.
			req.responseChan <- !checkedout
		case returned := <-kr.ret:
			kr.Ring[returned] = false
		}
	}
}

// Checkout tries to checkout a key. If the key is checked out,
// the key stays checked out and Checkout returns false. If the
// key is not checked out, then Checkout checks the Key out and
// returns true.
func (kr *mapKeyRing) Checkout(key string) bool {
	recieve := make(chan bool)
	kr.checkout <- &checkoutRequest{
		key:          key,
		responseChan: recieve,
	}
	return <-recieve
}

func (kr *mapKeyRing) Return(key string) {
	kr.ret <- key
}
