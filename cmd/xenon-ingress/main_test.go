package main

import (
	"context"
	"io"
	"net"
	"testing"
	"time"
)

func TestProxySkipsUnavailableBackend(t *testing.T) {
	backend, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	go func() {
		conn, e := backend.Accept()
		if e == nil {
			defer conn.Close()
			_, _ = io.Copy(conn, conn)
		}
	}()
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := probe.Addr().String()
	_ = probe.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = serve(ctx, address, []string{"127.0.0.1:1", backend.Addr().String()}) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		conn, e := net.DialTimeout("tcp", address, 100*time.Millisecond)
		if e == nil {
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(time.Second))
			_, _ = conn.Write([]byte("x"))
			data := make([]byte, 1)
			if _, e = io.ReadFull(conn, data); e == nil && string(data) == "x" {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("proxy did not forward through live backend")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
