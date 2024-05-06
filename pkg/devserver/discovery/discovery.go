package discovery

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

var (
	// Ports indicate the default ports that we attempt to scan on localhost
	// when discovering SDK-based endpoints
	Ports = []int{
		// le derp
		80, 443,
		// Rails, Express & N*xt routes
		3000, 3001, 3002, 3003, 3004, 3005, 3006, 3007, 3008, 3009, 3010,
		// Django
		5000,
		// Vite/SvelteKit
		5173,
		// Other common ports
		8000, 8080, 8081, 8888,
		// Redwood
		8910, 8911, 8912, 8913, 8914, 8915,
	}

	// Paths indicate the paths we attempt to hit when a web server is available.
	// These are the default, recommended paths for hosting Inngest routes.
	Paths = []string{
		// Defaults
		"/api/inngest",
		"/x/inngest",
		// Netlify
		"/.netlify/functions/inngest",
		// Redwood
		"/.redwood/functions/inngest",
	}

	timeout = 2 * time.Second

	hc = http.Client{
		Timeout: timeout,
	}

	// urls lists all auto-discovered URLs to the error connecting
	urls    = map[string]struct{}{}
	urlLock sync.Mutex
)

func init() {
	// Use the PORT env variable, if defined.
	if ps := os.Getenv("PORT"); ps != "" {
		num, _ := strconv.Atoi(ps)
		Ports = append(Ports, num)
	}
}

// Autodiscover attempts to automatically discover SDK endpoints running on
// the local machine, using a combination of the default supported ports
// and default API endpoints above.
func Autodiscover(ctx context.Context) map[string]struct{} {
	urlLock.Lock()

	ports := openPorts(ctx)
	for _, port := range ports {
		for _, path := range Paths {
			// These requests _should_ be fast as we know a port is open,
			// so we do these sequentially.
			url := fmt.Sprintf("http://127.0.0.1:%d%s", port, path)
			if err := checkURL(ctx, url); err == nil {
				if _, ok := urls[url]; !ok {
					// only add if the URL doesn't exist;  this ensures
					// we don't overwrite errors.
					urls[url] = struct{}{}
				}
			}
		}
	}

	urlLock.Unlock()

	return URLs()
}

// URLs returns a list of auto-discovered URLs
func URLs() map[string]struct{} {
	urlLock.Lock()
	defer urlLock.Unlock()
	copied := map[string]struct{}{}
	for k, v := range urls {
		copied[k] = v
	}
	return copied
}

// checkURL attempts to discover whether there's an SDK hosted at the given url
// by checking the presence of the "x-inngest-sdk" header.
func checkURL(ctx context.Context, url string) error {
	resp, err := hc.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.Header.Get("x-inngest-sdk") != "" {
		// This is valid.
		return nil
	}
	return fmt.Errorf("SDK header not found")
}

// openPorts simultaneously checks all supported localhost ports to see if
// any are open. This allows us to filter the default ports to only those
// that are serving connections prior to making HTTP requests.
func openPorts(ctx context.Context) []int {
	results := []int{}
	// Create a buffered channel with the number of ports, letting us push
	// valid ports without reading from the channel at the same time.
	found := make(chan int, len(Ports))

	wg := sync.WaitGroup{}
	for _, port := range Ports {
		wg.Add(1)
		go func(port int) {
			defer wg.Done()
			err := checkPort(port)
			if err == nil {
				found <- port
			}
		}(port)
	}
	wg.Wait()
	close(found)

	// Read all results from the port after the connections are made.
	for port := range found {
		results = append(results, port)
	}

	return results
}

// checkPort makes a tcp connection to the given port on localhost.
func checkPort(port int) error {
	conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), timeout)
	if err != nil {
		return err
	}
	if conn != nil {
		defer conn.Close()
		return nil
	}
	return fmt.Errorf("error connecting")
}
