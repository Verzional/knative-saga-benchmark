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

type InventoryRequest struct {
	OrderID        string  `json:"order_id"`
	SKU            string  `json:"sku"`
	Quantity       int     `json:"quantity"`
	Amount         float64 `json:"amount"`
	TriggerFailure bool    `json:"trigger_failure"`
}

type InventoryResponse struct {
	ReservationID string `json:"reservation_id,omitempty"`
	OrderID       string `json:"order_id"`
	Status        string `json:"status"`
	Error         string `json:"error,omitempty"`
}

var (
	reserveCount     uint64
	rejectCount      uint64
	releaseCount     uint64
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
		w.Write([]byte(`{"status":"healthy","service":"inventory-service","architecture":"temporal-orchestration","enhanced":true}`))
	})

	http.HandleFunc("/reserve", handleReserve)
	http.HandleFunc("/release", handleRelease)
	http.HandleFunc("/metrics", handleMetrics)

	log.Printf("[InventoryService-Temporal] Listening on port %s (pprof at /debug/pprof)...", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}

func handleReserve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req InventoryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	traceParent := getOrGenerateTraceparent(r)
	cacheKey := fmt.Sprintf("reserve:%s", req.OrderID)
	if cached, ok := idempotencyCache.Load(cacheKey); ok {
		log.Printf("[InventoryService] Idempotency cache hit for order_id=%s", req.OrderID)
		res := cached.(InventoryResponse)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("traceparent", traceParent)
		if res.Status == "REJECTED" {
			w.WriteHeader(http.StatusUnprocessableEntity)
		} else {
			w.WriteHeader(http.StatusOK)
		}
		json.NewEncoder(w).Encode(res)
		return
	}

	// Poison Pill Logic per research parameters
	isPoisonPill := req.TriggerFailure || req.Quantity < 0 || req.Amount < 0 ||
		strings.Contains(strings.ToUpper(req.SKU), "POISON-PILL")

	if isPoisonPill {
		atomic.AddUint64(&rejectCount, 1)
		log.Printf("[InventoryService] POISON PILL DETECTED! Rejecting order_id=%s, sku=%s, trace=%s",
			req.OrderID, req.SKU, traceParent)

		res := InventoryResponse{
			OrderID: req.OrderID,
			Status:  "REJECTED",
			Error:   "ERR_INVENTORY_REJECTED: poison pill or negative quantity detected",
		}
		idempotencyCache.Store(cacheKey, res)

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("traceparent", traceParent)
		w.WriteHeader(http.StatusUnprocessableEntity)
		json.NewEncoder(w).Encode(res)
		return
	}

	atomic.AddUint64(&reserveCount, 1)
	reservationID := fmt.Sprintf("RES-%s", req.OrderID)
	log.Printf("[InventoryService] Inventory reserved: order_id=%s, sku=%s, qty=%d, res_id=%s, trace=%s",
		req.OrderID, req.SKU, req.Quantity, reservationID, traceParent)

	res := InventoryResponse{
		ReservationID: reservationID,
		OrderID:       req.OrderID,
		Status:        "RESERVED",
	}
	idempotencyCache.Store(cacheKey, res)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("traceparent", traceParent)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(res)
}

func handleRelease(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	atomic.AddUint64(&releaseCount, 1)

	var req struct {
		OrderID       string `json:"order_id"`
		ReservationID string `json:"reservation_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	traceParent := getOrGenerateTraceparent(r)
	log.Printf("[InventoryService] Inventory released (compensation): order_id=%s, res_id=%s, trace=%s",
		req.OrderID, req.ReservationID, traceParent)

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("traceparent", traceParent)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"order_id": req.OrderID,
		"status":   "RELEASED",
	})
}

func handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "# HELP inventory_reservations_total Total inventory reservations\n")
	fmt.Fprintf(w, "# TYPE inventory_reservations_total counter\n")
	fmt.Fprintf(w, "inventory_reservations_total %d\n", atomic.LoadUint64(&reserveCount))
	fmt.Fprintf(w, "# HELP inventory_rejections_total Total inventory rejections\n")
	fmt.Fprintf(w, "# TYPE inventory_rejections_total counter\n")
	fmt.Fprintf(w, "inventory_rejections_total %d\n", atomic.LoadUint64(&rejectCount))
	fmt.Fprintf(w, "# HELP inventory_releases_total Total inventory releases\n")
	fmt.Fprintf(w, "# TYPE inventory_releases_total counter\n")
	fmt.Fprintf(w, "inventory_releases_total %d\n", atomic.LoadUint64(&releaseCount))
}
