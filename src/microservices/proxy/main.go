package main

import (
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// config — настройки API Gateway из переменных окружения.
type config struct {
	port                   string
	monolithURL            string
	moviesServiceURL       string
	eventsServiceURL       string
	gradualMigration       bool
	moviesMigrationPercent int
	client                 *http.Client
}

func main() {
	cfg := loadConfig()
	rand.Seed(time.Now().UnixNano())

	mux := http.NewServeMux()
	mux.HandleFunc("/health", handleHealth)
	mux.HandleFunc("/", cfg.proxyHandler)

	log.Printf(
		"Starting proxy on :%s (gradual=%v, moviesPercent=%d%%)",
		cfg.port, cfg.gradualMigration, cfg.moviesMigrationPercent,
	)
	log.Fatal(http.ListenAndServe(":"+cfg.port, mux))
}

func loadConfig() *config {
	port := envOr("PORT", "8000")
	percent, err := strconv.Atoi(envOr("MOVIES_MIGRATION_PERCENT", "0"))
	if err != nil || percent < 0 {
		percent = 0
	}
	if percent > 100 {
		percent = 100
	}

	return &config{
		port:                   port,
		monolithURL:            strings.TrimRight(envOr("MONOLITH_URL", "http://localhost:8080"), "/"),
		moviesServiceURL:       strings.TrimRight(envOr("MOVIES_SERVICE_URL", "http://localhost:8081"), "/"),
		eventsServiceURL:       strings.TrimRight(envOr("EVENTS_SERVICE_URL", "http://localhost:8082"), "/"),
		gradualMigration:       strings.EqualFold(envOr("GRADUAL_MIGRATION", "false"), "true"),
		moviesMigrationPercent: percent,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// handleHealth — проверка, что API Gateway жив.
func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("Strangler Fig Proxy is healthy"))
}

// proxyHandler — единая точка входа: выбирает backend и проксирует запрос как есть.
func (cfg *config) proxyHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/health" {
		handleHealth(w, r)
		return
	}

	targetBase := cfg.pickTarget(r.URL.Path)
	targetURL, err := url.Parse(targetBase)
	if err != nil {
		http.Error(w, "bad target url", http.StatusInternalServerError)
		return
	}

	outURL := *r.URL
	outURL.Scheme = targetURL.Scheme
	outURL.Host = targetURL.Host

	req, err := http.NewRequestWithContext(r.Context(), r.Method, outURL.String(), r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	copyHeaders(req.Header, r.Header)
	// Host должен совпадать с выбранным сервисом.
	req.Host = targetURL.Host

	log.Printf("%s %s -> %s", r.Method, r.URL.RequestURI(), targetBase)

	resp, err := cfg.client.Do(req)
	if err != nil {
		log.Printf("proxy error: %v", err)
		http.Error(w, "upstream unavailable", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// pickTarget решает, куда отправить запрос:
// - /api/movies* — монолит или Movies Service (Strangler Fig + фиче-флаг)
// - /api/events* — Events Service
// - всё остальное — монолит
func (cfg *config) pickTarget(path string) string {
	switch {
	case strings.HasPrefix(path, "/api/movies"):
		if cfg.shouldUseMoviesService() {
			return cfg.moviesServiceURL
		}
		return cfg.monolithURL
	case strings.HasPrefix(path, "/api/events"):
		return cfg.eventsServiceURL
	default:
		return cfg.monolithURL
	}
}

// shouldUseMoviesService — простая процентная маршрутизация.
// GRADUAL_MIGRATION=false → весь movies-трафик сразу в новый сервис.
// GRADUAL_MIGRATION=true  → только MOVIES_MIGRATION_PERCENT% уходит в movies.
func (cfg *config) shouldUseMoviesService() bool {
	if !cfg.gradualMigration {
		return true
	}
	if cfg.moviesMigrationPercent <= 0 {
		return false
	}
	if cfg.moviesMigrationPercent >= 100 {
		return true
	}
	return rand.Intn(100) < cfg.moviesMigrationPercent
}

func copyHeaders(dst, src http.Header) {
	for key, values := range src {
		// Hop-by-hop заголовки не пересылаем.
		if strings.EqualFold(key, "Connection") ||
			strings.EqualFold(key, "Keep-Alive") ||
			strings.EqualFold(key, "Proxy-Authenticate") ||
			strings.EqualFold(key, "Proxy-Authorization") ||
			strings.EqualFold(key, "Te") ||
			strings.EqualFold(key, "Trailers") ||
			strings.EqualFold(key, "Transfer-Encoding") ||
			strings.EqualFold(key, "Upgrade") {
			continue
		}
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}
