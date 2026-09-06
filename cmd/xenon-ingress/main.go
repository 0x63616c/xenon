// xenon-ingress is the proof harness's small TCP load balancer. Production
// deployments may use their platform load balancer instead.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

type proxy struct {
	backends []string
	mu       sync.Mutex
	next     int
}

func (p *proxy) backend() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	start := p.next
	p.next = (p.next + 1) % len(p.backends)
	ordered := make([]string, len(p.backends))
	for i := range ordered {
		ordered[i] = p.backends[(start+i)%len(p.backends)]
	}
	return ordered
}

func (p *proxy) handle(client net.Conn) {
	defer client.Close()
	var upstream net.Conn
	for _, address := range p.backend() {
		conn, err := net.DialTimeout("tcp", address, time.Second)
		if err == nil {
			upstream = conn
			break
		}
	}
	if upstream == nil {
		return
	}
	defer upstream.Close()
	done := make(chan struct{}, 1)
	go func() {
		_, _ = io.Copy(upstream, client)
		if c, ok := upstream.(*net.TCPConn); ok {
			_ = c.CloseWrite()
		}
		done <- struct{}{}
	}()
	_, _ = io.Copy(client, upstream)
	if c, ok := client.(*net.TCPConn); ok {
		_ = c.CloseWrite()
	}
	<-done
}

func serve(ctx context.Context, listen string, backends []string) error {
	if len(backends) == 0 {
		return fmt.Errorf("at least one backend required")
	}
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		return err
	}
	defer listener.Close()
	go func() { <-ctx.Done(); _ = listener.Close() }()
	p := &proxy{backends: backends}
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go p.handle(conn)
	}
}

func main() {
	listen := flag.String("listen", "", "listen address")
	backends := flag.String("backends", "", "comma-separated backend addresses")
	flag.Parse()
	if *listen == "" || *backends == "" || flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "--listen and --backends required")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := serve(ctx, *listen, strings.Split(*backends, ",")); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
