package main

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type BenchResult struct {
	name       string
	operations uint64
	elapsedMs  float64
	nsPerOp    float64
	opsPerSec  float64
}

func printResult(r BenchResult) {
	fmt.Printf("%-46s | %10d ops | %8.2f ms | %8.1f ns/op | %10.2f M ops/s\n",
		r.name, r.operations, r.elapsedMs, r.nsPerOp, r.opsPerSec/1e6)
}

// 1. Unbuffered Rendezvous Channel Ping-Pong (100,000 messages)
func benchUnbufferedPingPong() BenchResult {
	const kMessages = 100_000
	ch := make(chan int, 0)
	var wg sync.WaitGroup
	wg.Add(2)

	t0 := time.Now()

	go func() {
		for i := 0; i < kMessages; i++ {
			ch <- 1
		}
		close(ch)
		wg.Done()
	}()

	go func() {
		var sum uint64
		for v := range ch {
			sum += uint64(v)
		}
		if sum != kMessages {
			panic("sum mismatch")
		}
		wg.Done()
	}()

	wg.Wait()
	elapsed := time.Since(t0)
	ms := float64(elapsed.Nanoseconds()) / 1e6
	ns := float64(elapsed.Nanoseconds()) / float64(kMessages)
	return BenchResult{"1. Unbuffered Channel Rendezvous (cap=0)", kMessages, ms, ns, (float64(kMessages) * 1000.0) / ms}
}

// 2. Buffered Channel High-Throughput (1,000,000 messages, cap=256)
func benchBufferedChannelThroughput() BenchResult {
	const kMessages = 1_000_000
	ch := make(chan int, 256)
	var wg sync.WaitGroup
	wg.Add(2)

	t0 := time.Now()

	go func() {
		for i := 0; i < kMessages; i++ {
			ch <- 1
		}
		close(ch)
		wg.Done()
	}()

	go func() {
		var sum uint64
		for v := range ch {
			sum += uint64(v)
		}
		if sum != kMessages {
			panic("sum mismatch")
		}
		wg.Done()
	}()

	wg.Wait()
	elapsed := time.Since(t0)
	ms := float64(elapsed.Nanoseconds()) / 1e6
	ns := float64(elapsed.Nanoseconds()) / float64(kMessages)
	return BenchResult{"2. Buffered Channel Throughput (cap=256)", kMessages, ms, ns, (float64(kMessages) * 1000.0) / ms}
}

// 3. Spawn & Wait 50,000 Goroutines
func benchMassiveSpawn() BenchResult {
	const kRoutines = 50_000
	var counter atomic.Uint64
	var wg sync.WaitGroup
	wg.Add(kRoutines)

	t0 := time.Now()
	for i := 0; i < kRoutines; i++ {
		go func() {
			counter.Add(1)
			wg.Done()
		}()
	}
	wg.Wait()
	elapsed := time.Since(t0)

	if counter.Load() != kRoutines {
		panic("counter mismatch")
	}
	ms := float64(elapsed.Nanoseconds()) / 1e6
	ns := float64(elapsed.Nanoseconds()) / float64(kRoutines)
	return BenchResult{"3. Spawn & Join 50,000 Goroutines", kRoutines, ms, ns, (float64(kRoutines) * 1000.0) / ms}
}

// 4. High-Contention sync.Mutex (100 goroutines x 2,000 locks = 200,000 ops)
func benchContendedMutex() BenchResult {
	const kRoutines = 100
	const kLocksPerRoutine = 2_000
	const kTotalOps = kRoutines * kLocksPerRoutine

	var mtx sync.Mutex
	var sharedVal uint64
	var wg sync.WaitGroup
	wg.Add(kRoutines)

	t0 := time.Now()
	for r := 0; r < kRoutines; r++ {
		go func() {
			for i := 0; i < kLocksPerRoutine; i++ {
				mtx.Lock()
				sharedVal++
				mtx.Unlock()
			}
			wg.Done()
		}()
	}
	wg.Wait()
	elapsed := time.Since(t0)

	if sharedVal != kTotalOps {
		panic("mutex counter mismatch")
	}
	ms := float64(elapsed.Nanoseconds()) / 1e6
	ns := float64(elapsed.Nanoseconds()) / float64(kTotalOps)
	return BenchResult{"4. Contended sync.Mutex (100 goroutines)", kTotalOps, ms, ns, (float64(kTotalOps) * 1000.0) / ms}
}

// 5. Cooperative Yield Latency (100,000 runtime.Gosched() on GOMAXPROCS=1)
func benchContextSwitchYield() BenchResult {
	const kYields = 100_000
	prevProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(prevProcs)

	var wg sync.WaitGroup
	wg.Add(1)
	t0 := time.Now()
	go func() {
		for i := 0; i < kYields; i++ {
			runtime.Gosched()
		}
		wg.Done()
	}()
	wg.Wait()
	elapsed := time.Since(t0)

	ms := float64(elapsed.Nanoseconds()) / 1e6
	ns := float64(elapsed.Nanoseconds()) / float64(kYields)
	return BenchResult{"5. User-Space Context Switch (Gosched)", kYields, ms, ns, (float64(kYields) * 1000.0) / ms}
}

func main() {
	fmt.Println("==========================================================================================================")
	fmt.Printf("  Go Native Runtime Benchmark Suite (GOMAXPROCS: %d, Go Version: %s)\n", runtime.GOMAXPROCS(0), runtime.Version())
	fmt.Println("==========================================================================================================")

	printResult(benchUnbufferedPingPong())
	printResult(benchBufferedChannelThroughput())
	printResult(benchMassiveSpawn())
	printResult(benchContendedMutex())
	printResult(benchContextSwitchYield())

	fmt.Println("==========================================================================================================")
}
