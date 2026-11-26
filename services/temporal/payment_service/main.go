package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof" // Runtime profiling exposed on /debug/pprof
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
)

type PaymentRequest struct {
	OrderID       string  `json:"order_id"`
	CustomerID    string  `json:"customer_id"`
	Amount        float64 `json:"amount"`
	PaymentMethod string  `json:"payment_method"`
}

type PaymentResponse struct {
	PaymentID string  `json:"payment_id"`
	OrderID   string  `json:"order_id"`
	Status    string  `json:"status"`
	Amount    float64 `json:"amount"`
}

type RefundRequest struct {
	OrderID   string  `json:"order_id"`
	PaymentID string  `json:"payment_id"`
	Amount    float64 `json:"amount"`
	Reason    string  `json:"reason"`
}

type RefundResponse struct {
	RefundID string `json:"refund_id"`
	OrderID  string `json:"order_id"`
	Status   string `json:"status"`
}

var (
	requestCount     uint64
	refundCount      uint64
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
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"healthy","service":"payment-service","architecture":"temporal-orchestration","enhanced":true}`))
	})

	http.HandleFunc("/pay", handlePayment)
	http.HandleFunc("/refund", handleRefund)
	http.HandleFunc("/metrics", handleMetrics)

	log.Printf("[PaymentService-Temporal] Listening on port %s (pprof at /debug/pprof)...", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}

func handlePayment(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	atomic.AddUint64(&requestCount, 1)

	var req PaymentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	traceParent := getOrGenerateTraceparent(r)
	cacheKey := fmt.Sprintf("pay:%s", req.OrderID)
	if cached, ok := idempotencyCache.Load(cacheKey); ok {
		log.Printf("[PaymentService] Idempotency cache hit for order_id=%s", req.OrderID)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("traceparent", traceParent)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(cached)
		return
	}

	paymentID := fmt.Sprintf("PAY-%s", req.OrderID)
	log.Printf("[PaymentService] Payment processed: order_id=%s, amount=%.2f, payment_id=%s, trace=%s",
		req.OrderID, req.Amount, paymentID, traceParent)

	resp := PaymentResponse{
		PaymentID: paymentID,
		OrderID:   req.OrderID,
		Status:    "SUCCESS",
		Amount:    req.Amount,
	}
	idempotencyCache.Store(cacheKey, resp)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("traceparent", traceParent)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

func handleRefund(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	atomic.AddUint64(&refundCount, 1)

	var req RefundRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	traceParent := getOrGenerateTraceparent(r)
	cacheKey := fmt.Sprintf("refund:%s", req.OrderID)
	if cached, ok := idempotencyCache.Load(cacheKey); ok {
		log.Printf("[PaymentService] Idempotency cache hit for refund order_id=%s", req.OrderID)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("traceparent", traceParent)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(cached)
		return
	}

	refundID := fmt.Sprintf("REF-%s", req.OrderID)
	log.Printf("[PaymentService] Refund processed (compensation): order_id=%s, payment_id=%s, refund_id=%s, reason=%s, trace=%s",
		req.OrderID, req.PaymentID, refundID, req.Reason, traceParent)

	resp := RefundResponse{
		RefundID: refundID,
		OrderID:  req.OrderID,
		Status:   "REFUNDED",
	}
	idempotencyCache.Store(cacheKey, resp)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("traceparent", traceParent)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

func handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "# HELP payment_requests_total Total payment requests processed\n")
	fmt.Fprintf(w, "# TYPE payment_requests_total counter\n")
	fmt.Fprintf(w, "payment_requests_total %d\n", atomic.LoadUint64(&requestCount))
	fmt.Fprintf(w, "# HELP payment_refunds_total Total payment refunds processed\n")
	fmt.Fprintf(w, "# TYPE payment_refunds_total counter\n")
	fmt.Fprintf(w, "payment_refunds_total %d\n", atomic.LoadUint64(&refundCount))
}
