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

	"go.temporal.io/sdk/client"
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

var (
	temporalClient  client.Client
	temporalMutex   sync.RWMutex
	idempotencySeen sync.Map // Prevents duplicate execution anomalies
)

func getTemporalClient() (client.Client, error) {
	temporalMutex.RLock()
	if temporalClient != nil {
		temporalMutex.RUnlock()
		return temporalClient, nil
	}
	temporalMutex.RUnlock()

	temporalMutex.Lock()
	defer temporalMutex.Unlock()

	if temporalClient != nil {
		return temporalClient, nil
	}

	temporalHost := os.Getenv("TEMPORAL_HOST")
	if temporalHost == "" {
		temporalHost = "temporal-frontend.temporal.svc.cluster.local:7233"
	}

	c, err := client.Dial(client.Options{
		HostPort:  temporalHost,
		Namespace: "default",
		ConnectionOptions: client.ConnectionOptions{
			ConnectTimeout: 5 * time.Second,
			KeepAliveTime:  30 * time.Second,
		},
	})
	if err != nil {
		return nil, err
	}
	temporalClient = c
	return temporalClient, nil
}

func main() {
	// Expose pprof strictly on internal localhost port 6060 to prevent public ingress exploitation
	go func() {
		log.Println("[PPROF] Local runtime profiler listening on 127.0.0.1:6060")
		_ = http.ListenAndServe("127.0.0.1:6060", nil)
	}()

	_, err := getTemporalClient()
	if err != nil {
		log.Printf("Notice: initial dial to Temporal deferred (%v), will connect upon request", err)
	}
	defer func() {
		temporalMutex.Lock()
		if temporalClient != nil {
			temporalClient.Close()
		}
		temporalMutex.Unlock()
	}()

	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"healthy","architecture":"temporal-orchestration","bounded_context":true}`))
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

	log.Printf("Temporal Production Order Service listening on port %s", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server error: %v", err)
	}
}

func handleCreateOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req OrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON body: %v", err), http.StatusBadRequest)
		return
	}

	// Capture W3C distributed tracing context
	if tp := r.Header.Get("traceparent"); tp != "" {
		req.TraceParent = tp
	}

	// Idempotency validation
	if req.IdempotencyKey == "" {
		req.IdempotencyKey = fmt.Sprintf("idemp-%s", req.OrderID)
	}
	if _, loaded := idempotencySeen.LoadOrStore(req.IdempotencyKey, time.Now()); loaded {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"order_id":        req.OrderID,
			"status":          "COMPLETED_IDEMPOTENT",
			"idempotency_key": req.IdempotencyKey,
		})
		return
	}

	// Simulate cryptographic payload validation (realistic CPU computation)
	h := sha256.New()
	h.Write([]byte(fmt.Sprintf("%s:%s:%f", req.OrderID, req.CustomerID, req.Amount)))
	_ = hex.EncodeToString(h.Sum(nil))

	tc, err := getTemporalClient()
	if err != nil {
		http.Error(w, fmt.Sprintf("Temporal cluster unavailable: %v", err), http.StatusServiceUnavailable)
		return
	}

	workflowOptions := client.StartWorkflowOptions{
		ID:        fmt.Sprintf("order-saga-%s", req.OrderID),
		TaskQueue: "SAGA_ORDER_QUEUE",
	}

	// Bound workflow dispatch and execution to incoming request context with strict 8s timeout
	// Prevents goroutine accumulation and socket exhaustion during PostgreSQL pool saturation
	execCtx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()

	we, err := tc.ExecuteWorkflow(execCtx, workflowOptions, "CreateOrderSaga", req)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to initiate workflow execution: %v", err), http.StatusServiceUnavailable)
		return
	}

	var sagaResult struct {
		OrderID      string `json:"order_id"`
		Status       string `json:"status"`
		FailureStep  string `json:"failure_step,omitempty"`
		ErrorMessage string `json:"error_message,omitempty"`
	}

	err = we.Get(execCtx, &sagaResult)
	if err != nil {
		if execCtx.Err() == context.DeadlineExceeded {
			http.Error(w, "Workflow execution timeout under cluster backpressure", http.StatusGatewayTimeout)
			return
		}
		if sagaResult.Status == "COMPENSATED" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnprocessableEntity)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"order_id": req.OrderID,
				"status":   "COMPENSATED",
				"error":    sagaResult.ErrorMessage,
			})
			return
		}
		http.Error(w, fmt.Sprintf("Workflow execution failed: %v", err), http.StatusInternalServerError)
		return
	}

	if sagaResult.Status == "COMPENSATED" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnprocessableEntity)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"order_id": req.OrderID,
			"status":   "COMPENSATED",
			"error":    sagaResult.ErrorMessage,
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"order_id": req.OrderID,
		"status":   "COMPLETED",
	})
}
