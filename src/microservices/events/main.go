package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	topicMovieEvents   = "movie-events"
	topicUserEvents    = "user-events"
	topicPaymentEvents = "payment-events"
)

// Event — общая обёртка события, которую кладём в Kafka и отдаём в ответе API.
type Event struct {
	ID        string                 `json:"id"`
	Type      string                 `json:"type"`
	Timestamp string                 `json:"timestamp"`
	Payload   map[string]interface{} `json:"payload"`
}

// EventResponse — ответ API после записи в Kafka.
type EventResponse struct {
	Status    string `json:"status"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Event     Event  `json:"event"`
}

func main() {
	brokers := strings.Split(envOr("KAFKA_BROKERS", "localhost:9092"), ",")
	port := envOr("PORT", "8082")

	waitForKafka(brokers)

	// Consumer'ы читают свои топики и пишут сообщения в лог сервиса.
	go startConsumer(brokers, topicMovieEvents, "events-movie-consumer")
	go startConsumer(brokers, topicUserEvents, "events-user-consumer")
	go startConsumer(brokers, topicPaymentEvents, "events-payment-consumer")

	mux := http.NewServeMux()
	mux.HandleFunc("/api/events/health", handleHealth)
	mux.HandleFunc("/api/events/movie", makeEventHandler(brokers, topicMovieEvents, "movie", buildMovieEvent))
	mux.HandleFunc("/api/events/user", makeEventHandler(brokers, topicUserEvents, "user", buildUserEvent))
	mux.HandleFunc("/api/events/payment", makeEventHandler(brokers, topicPaymentEvents, "payment", buildPaymentEvent))

	log.Printf("Starting events service on port %s (brokers=%v)", port, brokers)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

type eventBuilder func(payload map[string]interface{}) (Event, error)

func makeEventHandler(brokers []string, topic, eventType string, build eventBuilder) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}

		var payload map[string]interface{}
		if err := json.Unmarshal(body, &payload); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid json")
			return
		}

		event, err := build(payload)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}
		event.Type = eventType
		if event.Timestamp == "" {
			event.Timestamp = time.Now().UTC().Format(time.RFC3339)
		}

		partition, offset, err := publishEvent(brokers, topic, event)
		if err != nil {
			log.Printf("failed to publish %s event: %v", eventType, err)
			writeJSONError(w, http.StatusInternalServerError, "failed to publish event")
			return
		}

		log.Printf("produced %s event id=%s topic=%s partition=%d offset=%d",
			eventType, event.ID, topic, partition, offset)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(EventResponse{
			Status:    "success",
			Partition: partition,
			Offset:    offset,
			Event:     event,
		})
	}
}

func buildMovieEvent(payload map[string]interface{}) (Event, error) {
	movieID, ok := asInt(payload["movie_id"])
	if !ok {
		return Event{}, fmt.Errorf("movie_id is required")
	}
	title, _ := payload["title"].(string)
	action, _ := payload["action"].(string)
	if title == "" || action == "" {
		return Event{}, fmt.Errorf("title and action are required")
	}

	return Event{
		ID:        fmt.Sprintf("movie-%d-%s", movieID, action),
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Payload:   payload,
	}, nil
}

func buildUserEvent(payload map[string]interface{}) (Event, error) {
	userID, ok := asInt(payload["user_id"])
	if !ok {
		return Event{}, fmt.Errorf("user_id is required")
	}
	action, _ := payload["action"].(string)
	if action == "" {
		return Event{}, fmt.Errorf("action is required")
	}
	ts, _ := payload["timestamp"].(string)
	if ts == "" {
		return Event{}, fmt.Errorf("timestamp is required")
	}

	return Event{
		ID:        fmt.Sprintf("user-%d-%s", userID, action),
		Timestamp: ts,
		Payload:   payload,
	}, nil
}

func buildPaymentEvent(payload map[string]interface{}) (Event, error) {
	paymentID, ok := asInt(payload["payment_id"])
	if !ok {
		return Event{}, fmt.Errorf("payment_id is required")
	}
	if _, ok := asInt(payload["user_id"]); !ok {
		return Event{}, fmt.Errorf("user_id is required")
	}
	if _, ok := asFloat(payload["amount"]); !ok {
		return Event{}, fmt.Errorf("amount is required")
	}
	status, _ := payload["status"].(string)
	ts, _ := payload["timestamp"].(string)
	if status == "" || ts == "" {
		return Event{}, fmt.Errorf("status and timestamp are required")
	}

	return Event{
		ID:        fmt.Sprintf("payment-%d-%s", paymentID, status),
		Timestamp: ts,
		Payload:   payload,
	}, nil
}

func publishEvent(brokers []string, topic string, event Event) (int, int64, error) {
	data, err := json.Marshal(event)
	if err != nil {
		return 0, 0, err
	}

	var (
		partition int
		offset    int64
	)

	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		Async:        false,
		Completion: func(messages []kafka.Message, err error) {
			if err == nil && len(messages) > 0 {
				partition = messages[0].Partition
				offset = messages[0].Offset
			}
		},
	}
	defer writer.Close()

	msg := kafka.Message{
		Key:   []byte(event.ID),
		Value: data,
		Time:  time.Now().UTC(),
	}

	var lastErr error
	for attempt := 1; attempt <= 10; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := writer.WriteMessages(ctx, msg)
		cancel()
		if err == nil {
			return partition, offset, nil
		}
		lastErr = err
		log.Printf("kafka write attempt %d failed: %v", attempt, err)
		time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
	}
	return 0, 0, lastErr
}

func startConsumer(brokers []string, topic, groupID string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
		StartOffset:    kafka.FirstOffset,
	})

	log.Printf("consumer started: topic=%s group=%s", topic, groupID)
	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("consumer error topic=%s: %v", topic, err)
			time.Sleep(2 * time.Second)
			continue
		}
		log.Printf(
			"consumed topic=%s partition=%d offset=%d key=%s value=%s",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value),
		)
	}
}

func waitForKafka(brokers []string) {
	for attempt := 1; attempt <= 30; attempt++ {
		conn, err := kafka.Dial("tcp", brokers[0])
		if err == nil {
			_ = conn.Close()
			log.Printf("kafka is ready at %s", brokers[0])
			return
		}
		log.Printf("waiting for kafka (%d/30): %v", attempt, err)
		time.Sleep(2 * time.Second)
	}
	log.Printf("warning: kafka may still be unavailable, starting anyway")
}

func writeJSONError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}

func asInt(v interface{}) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case int:
		return int64(n), true
	case int64:
		return n, true
	case json.Number:
		i, err := n.Int64()
		return i, err == nil
	default:
		return 0, false
	}
}

func asFloat(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}
