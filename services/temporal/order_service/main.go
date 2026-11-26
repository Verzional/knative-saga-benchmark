package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof" // Production runtime profiling exposed on /debug/pprof
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

var (
	temporalClient   client.Client
	orderCounter     uint64
	compCounter      uint64
	idempotencyCache sync.Map
)

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
	temporalHost := os.Getenv("TEMPORAL_HOST")
	if temporalHost == "" {
		temporalHost = "temporal-frontend.temporal.svc.cluster.local:7233"
	}

	// Initialize Temporal Client with retry
	var err error
	for i := 0; i < 5; i++ {
		temporalClient, err = client.Dial(client.Options{
			HostPort:  temporalHost,
			Namespace: "default",
		})
		if err == nil {
			log.Printf("Connected to Temporal at %s", temporalHost)
			break
		}
		log.Printf("Waiting for Temporal at %s (%v)... retry in 1s", temporalHost, err)
		time.Sleep(1 * time.Second)
	}

	if err != nil {
		log.Printf("Warning: Temporal connection failed on startup (%v). Will reconnect on demand.", err)
	} else {
		w := worker.New(temporalClient, "SAGA_ORDER_QUEUE", worker.Options{})
		w.RegisterWorkflow(CreateOrderSaga)
		w.RegisterActivity(ProcessPaymentActivity)
		w.RegisterActivity(RefundPaymentActivity)
		w.RegisterActivity(ReserveInventoryActivity)

		go func() {
			log.Println("Starting Temporal Worker on queue 'SAGA_ORDER_QUEUE'...")
			if err := w.Run(worker.InterruptCh()); err != nil {
				log.Printf("Temporal worker stopped: %v", err)
			}
		}()
	}

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"healthy","service":"order-service","architecture":"temporal-orchestration","enhanced":true}`))
	})

	http.HandleFunc("/orders", handleCreateOrder)
	http.HandleFunc("/metrics", handleMetrics)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("[OrderService-Temporal] Listening on port %s (pprof at /debug/pprof)...", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}

func handleCreateOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	atomic.AddUint64(&orderCounter, 1)

	var req OrderSagaRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid body: %v", err), http.StatusBadRequest)
		return
	}

	if req.OrderID == "" {
		req.OrderID = fmt.Sprintf("order-%s", uuid.New().String()[:8])
	}

	traceParent := getOrGenerateTraceparent(r)
	req.TraceParent = traceParent

	// Idempotency check
	if cached, ok := idempotencyCache.Load(req.OrderID); ok {
		log.Printf("[OrderService-Temporal] Idempotency cache hit for order_id=%s", req.OrderID)
		res := cached.(map[string]interface{})
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("traceparent", traceParent)
		if res["status"] == "COMPENSATED" {
			w.WriteHeader(http.StatusUnprocessableEntity)
		} else {
			w.WriteHeader(http.StatusOK)
		}
		json.NewEncoder(w).Encode(res)
		return
	}

	if temporalClient == nil {
		temporalHost := os.Getenv("TEMPORAL_HOST")
		if temporalHost == "" {
			temporalHost = "temporal-frontend.temporal.svc.cluster.local:7233"
		}
		var err error
		temporalClient, err = client.Dial(client.Options{
			HostPort:  temporalHost,
			Namespace: "default",
		})
		if err != nil {
			http.Error(w, fmt.Sprintf("Temporal connection failed: %v", err), http.StatusServiceUnavailable)
			return
		}
	}

	workflowOptions := client.StartWorkflowOptions{
		ID:        fmt.Sprintf("order-saga-%s", req.OrderID),
		TaskQueue: "SAGA_ORDER_QUEUE",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	we, err := temporalClient.ExecuteWorkflow(ctx, workflowOptions, CreateOrderSaga, req)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to initiate workflow: %v", err), http.StatusInternalServerError)
		return
	}

	var sagaResult OrderSagaResult
	err = we.Get(ctx, &sagaResult)
	if err != nil || sagaResult.Status == "COMPENSATED" || sagaResult.Status == "FAILED" {
		atomic.AddUint64(&compCounter, 1)
		errMsg := sagaResult.ErrorMessage
		if errMsg == "" && err != nil {
			errMsg = err.Error()
		}

		res := map[string]interface{}{
			"order_id": req.OrderID,
			"status":   "COMPENSATED",
			"error":    errMsg,
		}
		idempotencyCache.Store(req.OrderID, res)

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("traceparent", traceParent)
		w.WriteHeader(http.StatusUnprocessableEntity)
		json.NewEncoder(w).Encode(res)
		return
	}

	res := map[string]interface{}{
		"order_id": req.OrderID,
		"status":   "COMPLETED",
	}
	idempotencyCache.Store(req.OrderID, res)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("traceparent", traceParent)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(res)
}

func handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "# HELP orders_total Total orders received\n")
	fmt.Fprintf(w, "# TYPE orders_total counter\n")
	fmt.Fprintf(w, "orders_total %d\n", atomic.LoadUint64(&orderCounter))
	fmt.Fprintf(w, "# HELP orders_compensated_total Total orders compensated\n")
	fmt.Fprintf(w, "# TYPE orders_compensated_total counter\n")
	fmt.Fprintf(w, "orders_compensated_total %d\n", atomic.LoadUint64(&compCounter))
}
