package main

import (
	"bytes"
	"log"
	"net/http"
	"sync"
	"time"
)

func main() {
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	totalRequests := 50000
	concurrency := 100

	var wg sync.WaitGroup
	requestsPerWorker := totalRequests / concurrency

	start := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for j := 0; j < requestsPerWorker; j++ {
				req, err := http.NewRequest("POST", "http://localhost:9000/set", bytes.NewBuffer([]byte("testdata")))
				if err != nil {
					log.Println("req err:", err)
					continue
				}
				req.Header.Set("Content-Type", "application/octet-stream")

				resp, err := client.Do(req)
				if err != nil {
					log.Println("http err:", err)
					continue
				}
				_ = resp.Body.Close()
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)
	log.Printf("Completed %d requests in %s (%.2f req/sec)\n",
		totalRequests, elapsed, float64(totalRequests)/elapsed.Seconds())
}
