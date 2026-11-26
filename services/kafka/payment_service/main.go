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
	Reason         string  `json:"reason,omitempty"`
	TraceParent    string  `json:"traceparent,omitempty"`
}

var (
	kafkaWriter      *kafka.Writer
	paymentCount     uint64
	refundCount      uint64
	idempotencyCache sync.Map // map[orderID]map[string]interface{}
	httpClient       = &http.Client{Timeout: 10 * time.Second}
)

func getInventoryURL() string {
	url := os.Getenv("INVENTORY_SERVICE_URL")
	if url == "" {
		url = "http://inventory.default.svc.cluster.local"
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
		Topic:        "payment-events",
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
		w.Write([]byte(`{"status":"healthy","service":"payment-service","architecture":"kafka-choreography","enhanced":true}`))
	})

	http.HandleFunc("/events", handleCloudEvent)
	http.HandleFunc("/metrics", handleMetrics)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("[PaymentService-Kafka] Listening on port %s (pprof at /debug/pprof)...", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}

func listenKafka(broker string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    "order-events",
		GroupID:  "payment-service-group",
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
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(res)
}

func processEvent(ce CloudEvent) map[string]interface{} {
	var payload OrderPayload
	_ = json.Unmarshal(ce.Data, &payload)
	if payload.TraceParent == "" {
		payload.TraceParent = ce.TraceParent
	}

	cacheKey := fmt.Sprintf("%s:%s", ce.Type, payload.OrderID)
	if cached, ok := idempotencyCache.Load(cacheKey); ok {
		log.Printf("[PaymentService] Idempotency cache hit for key=%s", cacheKey)
		return cached.(map[string]interface{})
	}

	var res map[string]interface{}

	switch ce.Type {
	case "order.created":
		atomic.AddUint64(&paymentCount, 1)
		payload.PaymentID = fmt.Sprintf("PAY-%s", payload.OrderID)
		log.Printf("[PaymentService] Debiting account: order_id=%s, amount=%.2f, payment_id=%s, trace=%s",
			payload.OrderID, payload.Amount, payload.PaymentID, payload.TraceParent)

		publishEvent("payment.processed", payload.OrderID, payload.TraceParent, payload)
		forwardEvent(fmt.Sprintf("%s/events", getInventoryURL()), "payment.processed", payload.TraceParent, payload)
		res = map[string]interface{}{"status": "PAYMENT_PROCESSED", "order_id": payload.OrderID, "payment_id": payload.PaymentID}

	case "inventory.failed":
		atomic.AddUint64(&refundCount, 1)
		log.Printf("[PaymentService] Self-compensating REFUND triggered: order_id=%s, amount=%.2f, trace=%s",
			payload.OrderID, payload.Amount, payload.TraceParent)

		publishEvent("payment.refunded", payload.OrderID, payload.TraceParent, payload)
		forwardEvent(fmt.Sprintf("%s/events", getOrderURL()), "payment.refunded", payload.TraceParent, payload)
		res = map[string]interface{}{"status": "PAYMENT_REFUNDED", "order_id": payload.OrderID}

	default:
		res = map[string]interface{}{"status": "IGNORED"}
	}

	idempotencyCache.Store(cacheKey, res)
	return res
}

func publishEvent(eventType, key, traceParent string, payload interface{}) {
	dataBytes, _ := json.Marshal(payload)
	ce := CloudEvent{
		SpecVersion:     "1.0",
		ID:              uuid.New().String(),
		Source:          "payment-service",
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
		Source:          "payment-service",
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
	fmt.Fprintf(w, "# HELP payment_processed_total Total payments processed\n")
	fmt.Fprintf(w, "# TYPE payment_processed_total counter\n")
	fmt.Fprintf(w, "payment_processed_total %d\n", atomic.LoadUint64(&paymentCount))
	fmt.Fprintf(w, "# HELP payment_refunds_total Total payment refunds\n")
	fmt.Fprintf(w, "# TYPE payment_refunds_total counter\n")
	fmt.Fprintf(w, "payment_refunds_total %d\n", atomic.LoadUint64(&refundCount))
}
