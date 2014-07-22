package keyring

import (
	"sync"
	"testing"
)

func TestKeyRing(t *testing.T) {
	kr := New()
	w := sync.WaitGroup{}

	kr.Checkout("test")
	w.Add(1)
	go func() {
		defer w.Done()
		// Checkouts when the key is checked out should return false
		if kr.Checkout("test") {
			t.Fatal("expected test to be checkout out but wasnt")
		}
		// and not mutate the state.
		if kr.Checkout("test") {
			t.Fatal("expected test to be checkout out but wasnt")
		}
	}()
	w.Wait()

	kr.Return("test")
	w.Add(1)
	go func() {
		defer w.Done()
		// Checkouts when the key is not checked out should return true
		if !kr.Checkout("test") {
			t.Fatal("expected test to be checked in but wasnt")
		}
		// and mutate the state to be checked out.
		if kr.Checkout("test") {
			t.Fatal("expected test to be checked out but wasnt")
		}
	}()
	w.Wait()
}
