package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type TicketSystem struct {
	totalTickets     int32
	availableTickets int32
	mu               sync.Mutex
	bookings         map[string]*Booking
}

type Booking struct {
	ID        string
	UserID    string
	Timestamp time.Time
}

type BookingResult struct {
	UserID    string
	Success   bool
	BookingID string
	Error     error
}

func NewTicketSystem(totalTickets int32) *TicketSystem {
	return &TicketSystem{
		totalTickets:     totalTickets,
		availableTickets: totalTickets,
		bookings:         make(map[string]*Booking),
	}
}

func (ts *TicketSystem) BookTicket(userID string) BookingResult {
	// network latency
	time.Sleep(time.Microsecond * time.Duration(rand.Intn(100)))

	// non-blocking read
	if atomic.LoadInt32(&ts.availableTickets) <= 0 {
		return BookingResult{
			UserID:  userID,
			Success: false,
			Error:   fmt.Errorf("no tickets available"),
		}
	}

	// locking
	ts.mu.Lock()
	defer ts.mu.Unlock()

	// double check
	if ts.availableTickets <= 0 {
		return BookingResult{
			UserID:  userID,
			Success: false,
			Error:   fmt.Errorf("no tickets available"),
		}
	}

	// create
	bookingID := fmt.Sprintf("BOOK-%s-%d", userID, time.Now().UnixNano())
	booking := &Booking{
		ID:        bookingID,
		UserID:    userID,
		Timestamp: time.Now(),
	}

	// update
	ts.bookings[bookingID] = booking
	atomic.AddInt32(&ts.availableTickets, -1)

	return BookingResult{
		UserID:    userID,
		Success:   true,
		BookingID: bookingID,
	}
}

func (ts *TicketSystem) GetStats() (totalBooked int32, available int32) {
	available = atomic.LoadInt32(&ts.availableTickets)
	totalBooked = ts.totalTickets - available
	return
}

// worker to process request from a channel
type Worker struct {
	id       int
	requests <-chan string
	results  chan<- BookingResult
	ts       *TicketSystem
	wg       *sync.WaitGroup
}

// start booking requests
func (w *Worker) Start(ctx context.Context) {
	done := false
	for !done {
		select {
		case userID, ok := <-w.requests:
			if !ok {
				// channel closed, worker should exit
				done = true
				break
			}

			result := w.ts.BookTicket(userID)

			// send result
			select {
			case w.results <- result:
			case <-ctx.Done():
				done = true
			}

		case <-ctx.Done():
			done = true
		}
	}

	w.wg.Done()
}

type WorkerPool struct {
	workers  []*Worker
	requests chan string
	results  chan BookingResult
	wg       sync.WaitGroup
}

func NewWorkerPool(numWorkers int, ts *TicketSystem) *WorkerPool {
	pool := &WorkerPool{
		requests: make(chan string, numWorkers*2),
		results:  make(chan BookingResult, numWorkers*10),
		workers:  make([]*Worker, numWorkers),
	}

	for i := 0; i < numWorkers; i++ {
		pool.workers[i] = &Worker{
			id:       i,
			requests: pool.requests,
			results:  pool.results,
			ts:       ts,
			wg:       &pool.wg,
		}
	}

	return pool
}

// start all the workers
func (p *WorkerPool) Start(ctx context.Context) {
	for _, worker := range p.workers {
		p.wg.Add(1)
		go worker.Start(ctx)
	}
}

func (p *WorkerPool) Stop() {
	close(p.requests)
	p.wg.Wait()
	close(p.results)
}

func (p *WorkerPool) SubmitRequest(userID string) {
	p.requests <- userID
}

func main() {
	const (
		totalTickets  = 50000
		totalRequests = 75000
		numWorkers    = 1000
	)

	log.Println("🎫 Ticket Booking System Starting...")
	log.Printf("📊 Total Tickets Available: %d", totalTickets)
	log.Printf("👥 Total Booking Requests: %d", totalRequests)
	log.Printf("⚙️  Worker Pool Size: %d", numWorkers)
	log.Println(strings.Repeat("-", 50))

	startTime := time.Now()

	ticketSystem := NewTicketSystem(totalTickets)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	workerPool := NewWorkerPool(numWorkers, ticketSystem)
	workerPool.Start(ctx)

	var (
		successfulBookings int32
		failedBookings     int32
		processedRequests  int32
	)

	resultsDone := make(chan bool)
	go func() {
		for result := range workerPool.results {
			atomic.AddInt32(&processedRequests, 1)

			if result.Success {
				atomic.AddInt32(&successfulBookings, 1)
				if processedRequests%5000 == 0 {
					log.Printf("✅ Progress: %d/%d requests processed, %d tickets booked",
						atomic.LoadInt32(&processedRequests),
						totalRequests,
						atomic.LoadInt32(&successfulBookings))
				}
			} else {
				atomic.AddInt32(&failedBookings, 1)
			}
		}
		resultsDone <- true
	}()

	log.Println("🚀 Starting concurrent booking requests...")

	// client simulation (it has nothing to do with server)
	go func() {
		var requestWg sync.WaitGroup

		batchSize := 1000
		for i := 0; i < totalRequests; i += batchSize {
			requestWg.Add(1)
			go func(start int) {
				defer requestWg.Done()
				end := start + batchSize
				if end > totalRequests {
					end = totalRequests
				}

				for j := start; j < end; j++ {
					userID := fmt.Sprintf("USER-%06d", j)
					workerPool.SubmitRequest(userID)
				}
			}(i)
		}

		requestWg.Wait()
		workerPool.Stop()
	}()

	<-resultsDone

	duration := time.Since(startTime)
	bookedTickets, remainingTickets := ticketSystem.GetStats()

	log.Println(strings.Repeat("=", 50))
	log.Println("📈 BOOKING SYSTEM FINAL REPORT")
	log.Println(strings.Repeat("=", 50))
	log.Printf("⏱️  Total Time Taken: %v", duration)
	log.Printf("🎫 Total Tickets: %d", totalTickets)
	log.Printf("✅ Total Tickets Booked: %d", bookedTickets)
	log.Printf("❌ Total Tickets NOT Booked: %d", atomic.LoadInt32(&failedBookings))
	log.Printf("📊 Total Requests Processed: %d", atomic.LoadInt32(&processedRequests))
	log.Printf("🎯 Remaining Tickets: %d", remainingTickets)
	log.Printf("⚡ Requests per Second: %.2f", float64(totalRequests)/duration.Seconds())
	log.Printf("🔄 Average Time per Request: %v", duration/time.Duration(totalRequests))
	log.Println(strings.Repeat("=", 50))

	if bookedTickets == totalTickets && remainingTickets == 0 {
		log.Println("✅ SUCCESS: All tickets were booked correctly!")
	} else if bookedTickets < totalTickets {
		log.Printf("⚠️  WARNING: Only %d out of %d tickets were booked", bookedTickets, totalTickets)
	}

	if atomic.LoadInt32(&successfulBookings) != bookedTickets {
		log.Printf("❌ ERROR: Booking count mismatch! Counted: %d, Actual: %d",
			atomic.LoadInt32(&successfulBookings), bookedTickets)
	}
}
