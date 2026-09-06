package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/0x63616c/xenon/internal/proof/recorder"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func run() error {
	listen := flag.String("listen", "127.0.0.1:0", "numeric loopback listener")
	path := flag.String("journal", "", "new metadata journal")
	validate := flag.Bool("validate", false, "offline validator only")
	limit := flag.Int("capacity", 100000, "registration cap")
	flag.Parse()
	if *validate {
		r, e := recorder.Validate(*path)
		if e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(r)
	}
	host, _, e := net.SplitHostPort(*listen)
	if e != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		return fmt.Errorf("numeric loopback required")
	}
	r, e := recorder.New(*path, *limit)
	if e != nil {
		return e
	}
	l, e := net.Listen("tcp", *listen)
	if e != nil {
		return e
	}
	server := &http.Server{Handler: r, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second}
	errors := make(chan error, 1)
	go func() { errors <- server.Serve(l) }()
	json.NewEncoder(os.Stdout).Encode(map[string]string{"event": "ready", "address": l.Addr().String()})
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	select {
	case <-ctx.Done():
	case e = <-errors:
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdown); err != nil {
		server.Close()
		return err
	}
	if e != nil && e != http.ErrServerClosed {
		return e
	}
	return r.Close()
}
func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
