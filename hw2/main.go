package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"
)

func main() {
	from := flag.Int("from", 0, "начальный ID фильма")
	to := flag.Int("to", 0, "конечный ID фильма")
	workers := flag.Int("workers", 10, "количество работников")
	timeout := flag.Duration("timeout", 5*time.Second, "таймаут HTTP-запроса")
	flag.Parse()
	if *from == 0 || *to == 0 {
		fmt.Println("необходимо указать --from и --to")
		os.Exit(1)
	}
	if *workers <= 0 {
		fmt.Println("количество работников должно быть больше нуля")
		os.Exit(1)
	}
	if *from <= 0 {
		fmt.Println("начальный ID должен быть больше нуля")
		os.Exit(1)
	}

	if *to < *from {
		fmt.Println("конечный ID не может быть меньше начального")
		os.Exit(1)
	}

	if *timeout <= 0 {
		fmt.Println("таймаут должен быть больше нуля")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
	)
	defer stop()

	jobs := make(chan int)
	var wg sync.WaitGroup
	client := &http.Client{
		Timeout: *timeout,
	}
	for i := 0; i < *workers; i++ {
		wg.Add(1)

		go func(client *http.Client) {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return

				case id, ok := <-jobs:
					if !ok {
						return
					}

					if ctx.Err() != nil {
						return
					}

					movie, err := fetchMovie(
						ctx,
						id,
						client,
						"https://homeworksite.site",
					)

					if err != nil {
						if ctx.Err() != nil {
							return
						}

						fmt.Printf("ошибка загрузки фильма %d: %v\n", id, err)
						continue
					}

					fmt.Printf("%d — %s — %d — %s\n",
						movie.ID, movie.Title, movie.Year, movie.Director)
				}
			}
		}(client)
	}
sendJobs:
	for id := *from; id <= *to; id++ {
		select {
		case jobs <- id:
		case <-ctx.Done():
			break sendJobs
		}
	}

	close(jobs)
	wg.Wait()
}

func fetchMovie(ctx context.Context, id int, client *http.Client, baseURL string) (Movie, error) {
	req, err := http.NewRequestWithContext(
		ctx,
		"GET",
		fmt.Sprintf("%s/%d/info.0.json", baseURL, id),
		nil,
	)
	if err != nil {
		return Movie{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return Movie{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Movie{}, fmt.Errorf("HTTP status: %d", resp.StatusCode)
	}
	var movie Movie
	err = json.NewDecoder(resp.Body).Decode(&movie)
	if err != nil {
		return Movie{}, err
	}
	return movie, nil
}
