package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof" // Localhost profiler for runtime diagnostics
	"os"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/snappy"
)

type OrderRequest struct {
	OrderID        string  `json:"order_id"`
	CustomerID     string  `json:"customer_id"`
	SKU            string  `json:"sku"`
	Quantity       int     `json:"quantity"`
	Amount         float64 `json:"amount"`
	PaymentMethod  string  `json:"payment_method"`
	TriggerFailure bool    `json:"trigger_failure"`
	IdempotencyKey string  `json:"idempotencykey,omitempty"` // CNCF CloudEvents v1.0.2 spec
	TraceParent    string  `json:"traceparent,omitempty"`
}

type OrderEvent struct {
	SpecVersion    string       `json:"specversion"`
	ID             string       `json:"id"`
	Type           string       `json:"type"`
	Source         string       `json:"source"`
	Time           time.Time    `json:"time"`
	TraceParent    string       `json:"traceparent,omitempty"`
	IdempotencyKey string       `json:"idempotencykey,omitempty"` // Strictly lowercase alphanumeric per CNCF
	Data           OrderRequest `json:"data"`
}

var (
	kafkaWriter     *kafka.Writer
	pendingAcks     sync.Map // map[orderID]chan string
	idempotencySeen sync.Map // Prevents duplicate execution anomalies
)

func main() {
	// Expose pprof strictly on internal localhost port 6060
	go func() {
		log.Println("[PPROF] Local runtime profiler listening on 127.0.0.1:6060")
		_ = http.ListenAndServe("127.0.0.1:6060", nil)
	}()

	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "saga-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092"
	}

	kafkaWriter = &kafka.Writer{
		Addr:                   kafka.TCP(kafkaBrokers),
		Topic:                  "order-events",
		Balancer:               &kafka.LeastBytes{},
		BatchSize:              100,
		BatchTimeout:           10 * time.Millisecond,
		RequiredAcks:           kafka.RequireOne,
		Compression:            snappy.NewCompressionCodec(),
		AllowAutoTopicCreation: true,
		Async:                  false,
	}
	defer kafkaWriter.Close()

	go listenCompletions(kafkaBrokers)

	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"healthy","architecture":"kafka-choreography","cloudevents_v1":true}`))
	})

	mux.HandleFunc("/orders", handleCreateOrder)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  30 * time.Second,
	}

	log.Printf("Kafka Choreography Production Order Service listening on port %s", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server error: %v", err)
	}
}

func listenCompletions(broker string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{broker},
		Topic:          "inventory-events",
		GroupID:        "order-service-inbound",
		MinBytes:       10e3,
		MaxBytes:       10e6,
		CommitInterval: 50 * time.Millisecond,
	})
	defer r.Close()

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		var evt struct {
			Type string `json:"type"`
			Data struct {
				OrderID string `json:"order_id"`
			} `json:"data"`
		}
		if err := json.Unmarshal(m.Value, &evt); err == nil && evt.Data.OrderID != "" {
			if ch, ok := pendingAcks.Load(evt.Data.OrderID); ok {
				select {
				case ch.(chan string) <- evt.Type:
				default:
				}
			}
		}
	}
}

func handleCreateOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req OrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid body: %v", err), http.StatusBadRequest)
		return
	}

	// Capture W3C distributed tracing context
	if tp := r.Header.Get("traceparent"); tp != "" {
		req.TraceParent = tp
	}

	// Idempotency validation (CNCF compliant attribute key)
	if req.IdempotencyKey == "" {
		req.IdempotencyKey = fmt.Sprintf("idemp-%s", req.OrderID)
	}
	if _, loaded := idempotencySeen.LoadOrStore(req.IdempotencyKey, time.Now()); loaded {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"order_id":        req.OrderID,
			"status":          "COMPLETED_IDEMPOTENT",
			"idempotencykey":  req.IdempotencyKey,
		})
		return
	}

	// Simulate cryptographic payload validation (realistic CPU computation)
	h := sha256.New()
	h.Write([]byte(fmt.Sprintf("%s:%s:%f", req.OrderID, req.CustomerID, req.Amount)))
	_ = hex.EncodeToString(h.Sum(nil))

	event := OrderEvent{
		SpecVersion:    "1.0",
		ID:             fmt.Sprintf("evt-%s", req.OrderID),
		Type:           "order.created",
		Source:         "order-service",
		Time:           time.Now().UTC(),
		TraceParent:    req.TraceParent,
		IdempotencyKey: req.IdempotencyKey,
		Data:           req,
	}

	payload, err := json.Marshal(event)
	if err != nil {
		http.Error(w, fmt.Sprintf("Serialization error: %v", err), http.StatusInternalServerError)
		return
	}

	ackChan := make(chan string, 1)
	pendingAcks.Store(req.OrderID, ackChan)
	defer pendingAcks.Delete(req.OrderID)

	var kafkaHeaders []kafka.Header
	if req.TraceParent != "" {
		kafkaHeaders = append(kafkaHeaders, kafka.Header{
			Key:   "traceparent",
			Value: []byte(req.TraceParent),
		})
	}
	kafkaHeaders = append(kafkaHeaders, kafka.Header{
		Key:   "ce_specversion",
		Value: []byte("1.0"),
	}, kafka.Header{
		Key:   "ce_type",
		Value: []byte("order.created"),
	})

	err = kafkaWriter.WriteMessages(context.Background(), kafka.Message{
		Key:     []byte(req.OrderID),
		Value:   payload,
		Headers: kafkaHeaders,
	})
	if err != nil {
		http.Error(w, fmt.Sprintf("Kafka write error: %v", err), http.StatusServiceUnavailable)
		return
	}

	// Reusable timer prevents memory churn on hot path
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	select {
	case resultType := <-ackChan:
		if resultType == "inventory.failed" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnprocessableEntity)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"order_id": req.OrderID,
				"status":   "COMPENSATED",
				"event":    resultType,
			})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"order_id": req.OrderID,
			"status":   "COMPLETED",
			"event":    resultType,
		})
	case <-timer.C:
		// Asynchronous ACK in high-throughput tests (HTTP 202 Accepted)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"order_id": req.OrderID,
			"status":   "PROCESSING_ASYNC",
		})
	}
}
