package worker_pool

import (
	"sync"
)

type Job[Input any, Output any] struct {
	ID           int
	Data         Input
	batchResults chan Result[Output]
}

type Result[Output any] struct {
	JobID int
	Data  Output
	Err   error
}

type BatchRequest[Input any, Output any] struct {
	Jobs    []Job[Input, Output]
	Results chan []Result[Output]
}

type WorkerPool[Input any, Output any] struct {
	numWorkers int
	jobs       chan Job[Input, Output]
	scheduler  chan BatchRequest[Input, Output]
	quit       chan struct{}
	wg         sync.WaitGroup
	process    func(job Job[Input, Output], workerID int) Result[Output]
}

func NewWorkerPool[Input any, Output any](numWorkers int, process func(job Job[Input, Output], workerID int) Result[Output]) *WorkerPool[Input, Output] {
	return &WorkerPool[Input, Output]{
		numWorkers: numWorkers,
		process:    process,
		jobs:       make(chan Job[Input, Output]),
		scheduler:  make(chan BatchRequest[Input, Output]),
		quit:       make(chan struct{}),
	}
}

func (wp *WorkerPool[Input, Output]) Start() {
	// Launch fixed number of worker goroutines
	for i := 0; i < wp.numWorkers; i++ {
		wp.wg.Add(1)
		go func(workerID int) {
			defer wp.wg.Done()
			for {
				select {
				case job, ok := <-wp.jobs:
					if !ok {
						return
					}
					result := wp.process(job, workerID)
					job.batchResults <- result
				case <-wp.quit:
					return
				}
			}
		}(i)
	}

	// Launch scheduler goroutine
	go wp.runScheduler()
}

func (wp *WorkerPool[Input, Output]) runScheduler() {
	for batch := range wp.scheduler {
		batchSize := len(batch.Jobs)
		if batchSize == 0 {
			batch.Results <- []Result[Output]{}
			continue
		}

		batchResultsChan := make(chan Result[Output])

		// Create results slice for this batch
		batchResults := make([]Result[Output], batchSize)

		// Launch a single collector goroutine for this batch
		go func(results chan []Result[Output]) {
			// Collect all results for this batch
			for i := 0; i < batchSize; i++ {
				result := <-batchResultsChan
				batchResults[i] = result
			}
			close(batchResultsChan)
			// Send completed batch results
			results <- batchResults
		}(batch.Results)

		// Submit all jobs in this batch
		for _, job := range batch.Jobs {
			job.batchResults = batchResultsChan
			wp.jobs <- job
		}
	}
}

func (wp *WorkerPool[Input, Output]) SubmitBatch(jobs []Job[Input, Output]) []Result[Output] {
	resultsChan := make(chan []Result[Output], 1)
	wp.scheduler <- BatchRequest[Input, Output]{
		Jobs:    jobs,
		Results: resultsChan,
	}
	return <-resultsChan
}

func (wp *WorkerPool[Input, Output]) Stop() {
	close(wp.scheduler)
	close(wp.quit)
	wp.wg.Wait()
	close(wp.jobs)
}
