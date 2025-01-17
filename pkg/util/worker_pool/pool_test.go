package worker_pool

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestPool(t *testing.T) {
	numWorkers := 50_000

	type fnInput interface{}
	type fnOutput interface{}

	var TEST_completed = map[int]int{}
	var TEST_used = map[int]int{}
	var TEST_lock = sync.Mutex{}

	processJob := func(job Job[fnInput, fnOutput], workerID int) Result[fnOutput] {
		fmt.Printf("processing job %d on worker %d\n", job.ID, workerID)
		<-time.After(time.Second * 2)

		TEST_lock.Lock()
		TEST_completed[job.ID] += 1
		TEST_used[workerID] = 1
		TEST_lock.Unlock()

		// Simulate some work being done
		// Replace this with actual job processing logic
		return Result[fnOutput]{
			JobID: job.ID,
			Data:  fmt.Sprintf("Processed job %d by worker %d", job.ID, workerID),
			Err:   nil,
		}
	}

	// Create a worker pool with numWorkers workers
	p := NewWorkerPool[fnInput, fnOutput](numWorkers, processJob)
	p.Start()

	// Create two batches of jobs
	batch1 := make([]Job[fnInput, fnOutput], 5_000)
	for i := range batch1 {
		batch1[i] = Job[fnInput, fnOutput]{
			ID:   i,
			Data: fmt.Sprintf("Batch 1, Job %d", i),
		}
	}

	batch2 := make([]Job[fnInput, fnOutput], 80_000)
	for i := range batch2 {
		batch2[i] = Job[fnInput, fnOutput]{
			ID:   i + len(batch1), // Different ID range
			Data: fmt.Sprintf("Batch 2, Job %d", i),
		}
	}

	// Submit batches concurrently
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		results := p.SubmitBatch(batch1)
		fmt.Printf("Batch 1 results: %d jobs completed\n", len(results))
	}()

	go func() {
		defer wg.Done()
		results := p.SubmitBatch(batch2)
		fmt.Printf("Batch 2 results: %d jobs completed\n", len(results))
	}()

	wg.Wait()
	p.Stop()

	// All workers must be used
	for i := 0; i < numWorkers; i++ {
		require.Equal(t, 1, TEST_used[i], "worker %d was unused", i)
	}

	// All jobs must have completed
	for i := range batch1 {
		require.Equal(t, 1, TEST_completed[i], "did not process job %d", i)
	}

	for i := range batch2 {
		require.Equal(t, 1, TEST_completed[i+len(batch1)], "did not process job %d", i+len(batch1))
	}
}
