package main

import (
	"github.com/seefan/gossdb"
	"github.com/seefan/gossdb/conf"
	"sync"
	"time"
)

func main1() {
	pool, err := gossdb.NewPool(&conf.Config{
		Host:             "127.0.0.1",
		Port:             8888,
		MinPoolSize:      20,
		MaxPoolSize:      20,
		MaxWaitSize:      10000,
		AcquireIncrement: 5,
	})
	if err != nil {
		panic("create pool error")
	}
	defer pool.Close()
	if err := pool.Start(); err == nil {
		test(pool, 10, 100)
		test(pool, 50, 100)
		test(pool, 100, 100)
		test(pool, 200, 100)
		test(pool, 500, 100)
		test(pool, 800, 100)
		test(pool, 1000, 100)
		test(pool, 3000, 100)
		test(pool, 5000, 100)
	}
}

func test(pool *gossdb.Connectors, threadCount, callCount int) {
	now := time.Now()
	wait := new(sync.WaitGroup)
	for i := 0; i < threadCount; i++ {
		wait.Add(1)
		go func(p *gossdb.Connectors, w *sync.WaitGroup) {
			for j := 0; j < callCount; j++ {
				if c, e := p.NewClient(); e != nil {
					println(e.Error())
				} else {
					c.Close()
				}
			}
			w.Done()
		}(pool, wait)
	}
	wait.Wait()
	println("thread=", threadCount, "call=", callCount, "time=", time.Since(now).String())
}
