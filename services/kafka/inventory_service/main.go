package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof" // Production runtime profiling exposed on /debug/pprof
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type CloudEvent struct {
	SpecVersion     string          `json:"specversion"`
	ID              string          `json:"id"`
	Source          string          `json:"source"`
	Type            string          `json:"type"`
	DataContentType string          `json:"datacontenttype"`
	Time            time.Time       `json:"time"`
	TraceParent     string          `json:"traceparent,omitempty"`
	Data            json.RawMessage `json:"data"`
}

type OrderPayload struct {
	OrderID        string  `json:"order_id"`
	CustomerID     string  `json:"customer_id"`
	SKU            string  `json:"sku"`
	Quantity       int     `json:"quantity"`
	Amount         float64 `json:"amount"`
	PaymentMethod  string  `json:"payment_method"`
	TriggerFailure bool    `json:"trigger_failure"`
	PaymentID      string  `json:"payment_id,omitempty"`
	ReservationID  string  `json:"reservation_id,omitempty"`
	Reason         string  `json:"reason,omitempty"`
	TraceParent    string  `json:"traceparent,omitempty"`
}

var (
	kafkaWriter      *kafka.Writer
	reserveCount     uint64
	rejectCount      uint64
	idempotencyCache sync.Map // map[orderID]map[string]interface{}
	httpClient       = &http.Client{Timeout: 10 * time.Second}
)

func getPaymentURL() string {
	url := os.Getenv("PAYMENT_SERVICE_URL")
	if url == "" {
		url = "http://payment.default.svc.cluster.local"
	}
	return url
}

func getOrderURL() string {
	url := os.Getenv("ORDER_SERVICE_URL")
	if url == "" {
		url = "http://order.default.svc.cluster.local"
	}
	return url
}

func getOrGenerateTraceparent(r *http.Request) string {
	tp := r.Header.Get("traceparent")
	if tp != "" {
		return tp
	}
	traceID := strings.ReplaceAll(uuid.New().String(), "-", "")
	spanID := strings.ReplaceAll(uuid.New().String(), "-", "")[:16]
	return fmt.Sprintf("00-%s-%s-01", traceID, spanID)
}

func main() {
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "saga-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092"
	}

	kafkaWriter = &kafka.Writer{
		Addr:         kafka.TCP(kafkaBrokers),
		Topic:        "inventory-events",
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		Compression:  kafka.Snappy,
		WriteTimeout: 5 * time.Second,
	}
	defer kafkaWriter.Close()

	go listenKafka(kafkaBrokers)

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"healthy","service":"inventory-service","architecture":"kafka-choreography","enhanced":true}`))
	})

	http.HandleFunc("/events", handleCloudEvent)
	http.HandleFunc("/metrics", handleMetrics)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("[InventoryService-Kafka] Listening on port %s (pprof at /debug/pprof)...", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}

func listenKafka(broker string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    "payment-events",
		GroupID:  "inventory-service-group",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	defer r.Close()

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		var ce CloudEvent
		if err := json.Unmarshal(m.Value, &ce); err == nil {
			processEvent(ce)
		}
	}
}

func handleCloudEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Read error", http.StatusBadRequest)
		return
	}

	var ce CloudEvent
	if err := json.Unmarshal(bodyBytes, &ce); err != nil {
		http.Error(w, fmt.Sprintf("Invalid CloudEvent: %v", err), http.StatusBadRequest)
		return
	}

	if ce.TraceParent == "" {
		ce.TraceParent = getOrGenerateTraceparent(r)
	}

	res := processEvent(ce)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("traceparent", ce.TraceParent)
	if res["status"] == "INVENTORY_REJECTED" {
		w.WriteHeader(http.StatusUnprocessableEntity)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	json.NewEncoder(w).Encode(res)
}

func processEvent(ce CloudEvent) map[string]interface{} {
	var payload OrderPayload
	_ = json.Unmarshal(ce.Data, &payload)
	if payload.TraceParent == "" {
		payload.TraceParent = ce.TraceParent
	}

	if ce.Type != "payment.processed" {
		return map[string]interface{}{"status": "IGNORED"}
	}

	cacheKey := fmt.Sprintf("reserve:%s", payload.OrderID)
	if cached, ok := idempotencyCache.Load(cacheKey); ok {
		log.Printf("[InventoryService] Idempotency cache hit for order_id=%s", payload.OrderID)
		return cached.(map[string]interface{})
	}

	// Poison Pill Logic per research specifications
	isPoisonPill := payload.TriggerFailure || payload.Quantity < 0 || payload.Amount < 0 ||
		strings.Contains(strings.ToUpper(payload.SKU), "POISON-PILL")

	var res map[string]interface{}

	if isPoisonPill {
		atomic.AddUint64(&rejectCount, 1)
		payload.Reason = "ERR_INVENTORY_REJECTED: poison pill or negative quantity detected"
		log.Printf("[InventoryService] POISON PILL DETECTED! Emitting inventory.failed for order_id=%s, sku=%s, trace=%s",
			payload.OrderID, payload.SKU, payload.TraceParent)

		publishEvent("inventory.failed", payload.OrderID, payload.TraceParent, payload)
		forwardEvent(fmt.Sprintf("%s/events", getPaymentURL()), "inventory.failed", payload.TraceParent, payload)
		forwardEvent(fmt.Sprintf("%s/events", getOrderURL()), "inventory.failed", payload.TraceParent, payload)

		res = map[string]interface{}{
			"status":   "INVENTORY_REJECTED",
			"order_id": payload.OrderID,
			"error":    payload.Reason,
		}
	} else {
		atomic.AddUint64(&reserveCount, 1)
		payload.ReservationID = fmt.Sprintf("RES-%s", payload.OrderID)
		log.Printf("[InventoryService] Inventory reserved: order_id=%s, sku=%s, qty=%d, res_id=%s, trace=%s",
			payload.OrderID, payload.SKU, payload.Quantity, payload.ReservationID, payload.TraceParent)

		publishEvent("inventory.reserved", payload.OrderID, payload.TraceParent, payload)
		forwardEvent(fmt.Sprintf("%s/events", getOrderURL()), "inventory.reserved", payload.TraceParent, payload)

		res = map[string]interface{}{
			"status":         "INVENTORY_RESERVED",
			"order_id":       payload.OrderID,
			"reservation_id": payload.ReservationID,
		}
	}

	idempotencyCache.Store(cacheKey, res)
	return res
}

func publishEvent(eventType, key, traceParent string, payload interface{}) {
	dataBytes, _ := json.Marshal(payload)
	ce := CloudEvent{
		SpecVersion:     "1.0",
		ID:              uuid.New().String(),
		Source:          "inventory-service",
		Type:            eventType,
		DataContentType: "application/json",
		Time:            time.Now().UTC(),
		TraceParent:     traceParent,
		Data:            dataBytes,
	}
	ceBytes, _ := json.Marshal(ce)

	_ = kafkaWriter.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(key),
		Value: ceBytes,
		Headers: []kafka.Header{
			{Key: "traceparent", Value: []byte(traceParent)},
		},
	})
}

func forwardEvent(targetURL, eventType, traceParent string, payload interface{}) {
	dataBytes, _ := json.Marshal(payload)
	ce := CloudEvent{
		SpecVersion:     "1.0",
		ID:              uuid.New().String(),
		Source:          "inventory-service",
		Type:            eventType,
		DataContentType: "application/json",
		Time:            time.Now().UTC(),
		TraceParent:     traceParent,
		Data:            dataBytes,
	}
	ceBytes, _ := json.Marshal(ce)

	req, err := http.NewRequest(http.MethodPost, targetURL, bytes.NewReader(ceBytes))
	if err == nil {
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("traceparent", traceParent)
		resp, err := httpClient.Do(req)
		if err == nil {
			resp.Body.Close()
		}
	}
}

func handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "# HELP inventory_reserved_total Total inventory reservations\n")
	fmt.Fprintf(w, "# TYPE inventory_reserved_total counter\n")
	fmt.Fprintf(w, "inventory_reserved_total %d\n", atomic.LoadUint64(&reserveCount))
	fmt.Fprintf(w, "# HELP inventory_failed_total Total inventory rejections/poison pills\n")
	fmt.Fprintf(w, "# TYPE inventory_failed_total counter\n")
	fmt.Fprintf(w, "inventory_failed_total %d\n", atomic.LoadUint64(&rejectCount))
}
