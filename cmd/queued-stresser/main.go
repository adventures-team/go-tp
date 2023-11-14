// queued-stresser - parallel and asynchronous queued stress test.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	redialInterval = 10 * time.Millisecond
	pingInterval   = 5 * time.Second
	defaultTimeout = time.Second
)

// Options represents command line options.
type Options struct {
	MasterPort  string `yaml:"master"`
	AdminPort   string `yaml:"admin"`
	QueueID     int    `yaml:"queue"`
	Connections int    `yaml:"conn"`
	Pipeline    int
	Size        int
	Timeout     time.Duration
	Duration    time.Duration
	Refresh     time.Duration
	Debug       bool
}

func (opt *Options) bind() {
	flag.StringVar(&opt.MasterPort, "master", "", "queued master address:port")
	flag.StringVar(&opt.AdminPort, "admin", "", "queued admin address:port")
	flag.IntVar(&opt.QueueID, "queue", 123, "test queue number")
	flag.IntVar(&opt.Connections, "conn", 10, "number of parallel connections")
	flag.IntVar(&opt.Pipeline, "pipeline", 100, "number of requests being processed concurrently per connection")
	flag.IntVar(&opt.Size, "size", 128, "queue message size")
	flag.DurationVar(&opt.Timeout, "timeout", defaultTimeout, "timeout")
	flag.DurationVar(&opt.Duration, "duration", time.Minute, "test duration (use 0s for infinite run)")
	flag.DurationVar(&opt.Refresh, "refresh", time.Second, "stats refresh interval")
	flag.BoolVar(&opt.Debug, "debug", false, "easygo/iproto debug")
}

func main() {
	opt := &Options{}

	opt.bind()
	flag.Parse()

	if opt.MasterPort == "" || opt.AdminPort == "" {
		flag.Usage()
		log.Fatal("-master and -admin flags are mandatory")
	}

	_ = yaml.NewEncoder(os.Stdout).Encode(opt)

	ctx, cancel := setupContext(opt)

	defer cancel()

	queueCleanup(ctx, opt, "initial")

	defer func() {
		ctx2, cancel := setupContext(opt) // the old one may be already cancelled
		defer cancel()
		queueCleanup(ctx2, opt, "final")
	}()

	pool := NewConnPool(opt)

	go refreshStats(ctx, opt, pool)
	pool.Run(ctx)
}

func setupContext(opt *Options) (ctx context.Context, cancel func()) {
	ctx = context.Background()
	ctx, stopNotify := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	cancel = stopNotify

	if opt.Duration != time.Duration(0) {
		ctx2, ctxCancel := context.WithTimeout(ctx, opt.Duration)
		ctx = ctx2

		cancel = func() {
			stopNotify()
			ctxCancel()
		}
	}

	return ctx, cancel
}

func queueCleanup(ctx context.Context, opt *Options, reason string) {
	admin, err := NewAdmin(ctx, opt)
	if err != nil {
		log.Fatalf("error connecting admin port during %s cleanup: %v", reason, err)
	}

	defer admin.Close()

	if err = admin.DestroyStorage(ctx, opt.QueueID); err != nil {
		log.Fatalf("%s DestroyStorage: %v", reason, err)
	}

	log.Println(reason + " cleanup OK")
}

func refreshStats(ctx context.Context, opt *Options, pool *ConnPool) {
	ticker := time.NewTicker(opt.Refresh)
	oldCount := uint32(0) // have to keep separate counter because we can't simply reset reqID like other counters

	for {
		select {
		case <-ticker.C:
			count := pool.TotalCount()
			delta := count - oldCount
			oldCount = count

			kbytes := pool.SampleSize() / 1024
			duration, durationMax := pool.SampleDuration()
			sec := float64(time.Second) / float64(opt.Refresh)

			log.Printf("%d Kb sent, %.0f rps, %.0f Kb/s, avg: %.2f ms/req, max: %.f ms/req",
				kbytes, float64(delta)*sec,
				float64(kbytes)*sec, float64(duration)/float64(time.Millisecond)/float64(delta),
				float64(durationMax)/float64(time.Millisecond))

		case <-ctx.Done():
			log.Print("exiting")
			return
		}
	}
}
