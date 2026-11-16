package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	bridge "github.com/Neo1Noir/wecom-dify-go"
)

func run() error {
	demo := flag.Bool("demo", false, "run a loopback-only demo with no external API calls")
	status := flag.Bool("status", false, "print stored message-state counts without starting the service")
	resolve := flag.Int64("resolve", 0, "reconcile an uncertain message row after manually checking delivery")
	as := flag.String("as", "", "resolution: sent or failed; never resends")
	state := flag.String("state", "", "override the SQLite path")
	listen := flag.String("listen", "", "override listen address; demo mode requires a loopback IP")
	flag.Parse()
	var c bridge.Config
	var err error
	if *demo {
		path := *state
		if path == "" {
			dir, e := os.MkdirTemp("", "wecom-dify-demo-")
			if e != nil {
				return e
			}
			path = dir + "/state.db"
		}
		c = bridge.DemoConfig(path)
	} else if *status || *resolve != 0 {
		c.StatePath = *state
		if c.StatePath == "" {
			c.StatePath = os.Getenv("SQLITE_PATH")
		}
		if c.StatePath == "" {
			c.StatePath = "data/state.db"
		}
	} else {
		c, err = bridge.FromEnv()
		if err != nil {
			return err
		}
		if *state != "" {
			c.StatePath = *state
		}
	}
	if *listen != "" {
		if *demo {
			host, _, e := net.SplitHostPort(*listen)
			if e != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
				return fmt.Errorf("demo listen address must use a loopback IP")
			}
		}
		c.Listen = *listen
	}
	if *status || *resolve != 0 {
		if _, e := os.Stat(c.StatePath); e != nil {
			return e
		}
	}
	store, err := bridge.OpenStore(c.StatePath)
	if err != nil {
		return err
	}
	defer store.DB.Close()
	if *resolve != 0 {
		return store.Resolve(*resolve, *as)
	}
	if *status {
		counts, e := store.Counts()
		if e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(counts)
	}
	server := &bridge.Server{Config: c, Store: store, Platform: bridge.NewWeCom(c), Chat: bridge.NewDify(c), Log: slog.Default()}
	handler := server.Handler()
	if *demo {
		d := &bridge.Demo{}
		server.Platform = d
		server.Chat = d
		handler = d.Handler(server)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	workerDone := make(chan error, 1)
	go func() { workerDone <- server.Run(ctx) }()
	httpServer := &http.Server{Addr: c.Listen, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	serveDone := make(chan error, 1)
	go func() {
		slog.Info("listening", "address", c.Listen, "demo", *demo)
		serveDone <- httpServer.ListenAndServe()
	}()
	select {
	case err = <-serveDone:
		cancel()
	case err = <-workerDone:
		cancel()
		workerDone = nil
	case <-ctx.Done():
	}
	shutdown, stop := context.WithTimeout(context.Background(), 15*time.Second)
	defer stop()
	_ = httpServer.Shutdown(shutdown)
	if workerDone != nil {
		if e := <-workerDone; e != nil {
			return e
		}
	}
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
