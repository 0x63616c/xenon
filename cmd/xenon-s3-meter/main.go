// xenon-s3-meter is an optional local acceptance-proof helper.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/0x63616c/xenon/internal/proof/s3meter"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type config struct {
	Listen       string `json:"listen"`
	ReportListen string `json:"report_listen"`
	Target       string `json:"target"`
}

func run() error {
	path := flag.String("config", "proof/s3-meter/local.json", "committed local proof config")
	flag.Parse()
	f, e := os.Open(*path)
	if e != nil {
		return e
	}
	defer f.Close()
	d := json.NewDecoder(f)
	d.DisallowUnknownFields()
	var c config
	if e = d.Decode(&c); e != nil {
		return e
	}
	for _, a := range []string{c.Listen, c.ReportListen} {
		if e = s3meter.LoopbackAddress(a); e != nil {
			return e
		}
	}
	p, e := s3meter.New(c.Target)
	if e != nil {
		return e
	}
	defer p.Close()
	listener, e := net.Listen("tcp", c.Listen)
	if e != nil {
		return e
	}
	defer listener.Close()
	reports, e := net.Listen("tcp", c.ReportListen)
	if e != nil {
		return e
	}
	defer reports.Close()
	server := &http.Server{Handler: p, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 1 << 20}
	reportServer := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.URL.Path != "/report" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(p.Snapshot())
	}), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 1 << 20}
	errs := make(chan error, 2)
	go func() { errs <- server.Serve(listener) }()
	go func() { errs <- reportServer.Serve(reports) }()
	_ = json.NewEncoder(os.Stdout).Encode(map[string]string{"event": "ready", "listen": listener.Addr().String(), "report_listen": reports.Addr().String()})
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	select {
	case <-ctx.Done():
	case e = <-errs:
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdown); err != nil {
		_ = server.Close()
		e = err
	}
	_ = reportServer.Shutdown(shutdown)
	_ = json.NewEncoder(os.Stdout).Encode(p.Snapshot())
	if e == http.ErrServerClosed {
		return nil
	}
	return e
}
func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
