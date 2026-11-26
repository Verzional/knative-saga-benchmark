package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type OrderSagaRequest struct {
	OrderID        string  `json:"order_id"`
	CustomerID     string  `json:"customer_id"`
	SKU            string  `json:"sku"`
	Quantity       int     `json:"quantity"`
	Amount         float64 `json:"amount"`
	PaymentMethod  string  `json:"payment_method"`
	TriggerFailure bool    `json:"trigger_failure"`
	TraceParent    string  `json:"traceparent,omitempty"`
}

type OrderSagaResult struct {
	OrderID      string    `json:"order_id"`
	Status       string    `json:"status"` // "COMPLETED" or "COMPENSATED"
	FailureStep  string    `json:"failure_step,omitempty"`
	ErrorMessage string    `json:"error_message,omitempty"`
	FinishedAt   time.Time `json:"finished_at"`
}

// CreateOrderSaga orchestrates distributed saga with backward compensation
func CreateOrderSaga(ctx workflow.Context, req OrderSagaRequest) (*OrderSagaResult, error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 15 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1, // Immediate failure to execute backward compensation without delay
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	logger := workflow.GetLogger(ctx)
	logger.Info("Starting CreateOrderSaga", "order_id", req.OrderID, "traceparent", req.TraceParent)

	var compensations []func(workflow.Context)

	// Step 1: Process Payment
	var paymentRef string
	err := workflow.ExecuteActivity(ctx, ProcessPaymentActivity, req).Get(ctx, &paymentRef)
	if err != nil {
		logger.Error("Payment step failed", "order_id", req.OrderID, "error", err)
		return &OrderSagaResult{
			OrderID:      req.OrderID,
			Status:       "FAILED",
			FailureStep:  "Payment",
			ErrorMessage: err.Error(),
			FinishedAt:   workflow.Now(ctx),
		}, nil
	}

	// Register Payment Compensation (Refund)
	compensations = append(compensations, func(compCtx workflow.Context) {
		var refundID string
		_ = workflow.ExecuteActivity(compCtx, RefundPaymentActivity, req.OrderID, paymentRef, req.Amount, req.TraceParent).Get(compCtx, &refundID)
	})

	// Step 2: Reserve Inventory
	var invReservationID string
	err = workflow.ExecuteActivity(ctx, ReserveInventoryActivity, req).Get(ctx, &invReservationID)
	if err != nil {
		logger.Error("Inventory reservation failed, triggering backward compensation!", "order_id", req.OrderID, "error", err)

		// Execute compensations in reverse order
		compCtx, _ := workflow.NewDisconnectedContext(ctx)
		for i := len(compensations) - 1; i >= 0; i-- {
			compensations[i](compCtx)
		}

		return &OrderSagaResult{
			OrderID:      req.OrderID,
			Status:       "COMPENSATED",
			FailureStep:  "Inventory",
			ErrorMessage: err.Error(),
			FinishedAt:   workflow.Now(ctx),
		}, nil
	}

	return &OrderSagaResult{
		OrderID:    req.OrderID,
		Status:     "COMPLETED",
		FinishedAt: workflow.Now(ctx),
	}, nil
}

func getPaymentServiceURL() string {
	url := os.Getenv("PAYMENT_SERVICE_URL")
	if url == "" {
		url = "http://payment.default.svc.cluster.local"
	}
	return url
}

func getInventoryServiceURL() string {
	url := os.Getenv("INVENTORY_SERVICE_URL")
	if url == "" {
		url = "http://inventory.default.svc.cluster.local"
	}
	return url
}

var httpClient = &http.Client{
	Timeout: 12 * time.Second,
}

// ProcessPaymentActivity calls payment-service via HTTP with W3C traceparent
func ProcessPaymentActivity(ctx context.Context, req OrderSagaRequest) (string, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Executing ProcessPaymentActivity", "order_id", req.OrderID, "amount", req.Amount, "trace", req.TraceParent)

	paymentURL := fmt.Sprintf("%s/pay", getPaymentServiceURL())
	payloadBytes, _ := json.Marshal(map[string]interface{}{
		"order_id":       req.OrderID,
		"customer_id":    req.CustomerID,
		"amount":         req.Amount,
		"payment_method": req.PaymentMethod,
	})

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, paymentURL, bytes.NewReader(payloadBytes))
	if err != nil {
		return "", err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if req.TraceParent != "" {
		httpReq.Header.Set("traceparent", req.TraceParent)
	}

	resp, err := httpClient.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("payment service unreachable: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("payment service returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var res struct {
		PaymentID string `json:"payment_id"`
		Status    string `json:"status"`
	}
	_ = json.Unmarshal(bodyBytes, &res)
	if res.PaymentID == "" {
		res.PaymentID = fmt.Sprintf("PAY-%s", req.OrderID)
	}
	return res.PaymentID, nil
}

// RefundPaymentActivity performs compensatory refund by calling payment-service
func RefundPaymentActivity(ctx context.Context, orderID, paymentRef string, amount float64, traceParent string) (string, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Executing Compensatory RefundPaymentActivity", "order_id", orderID, "payment_ref", paymentRef, "trace", traceParent)

	refundURL := fmt.Sprintf("%s/refund", getPaymentServiceURL())
	payloadBytes, _ := json.Marshal(map[string]interface{}{
		"order_id":   orderID,
		"payment_id": paymentRef,
		"amount":     amount,
		"reason":     "Compensating transaction: Inventory step failed",
	})

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, refundURL, bytes.NewReader(payloadBytes))
	if err != nil {
		return "", err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if traceParent != "" {
		httpReq.Header.Set("traceparent", traceParent)
	}

	resp, err := httpClient.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("payment service refund unreachable: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	var res struct {
		RefundID string `json:"refund_id"`
	}
	_ = json.Unmarshal(bodyBytes, &res)
	return res.RefundID, nil
}

// ReserveInventoryActivity calls inventory-service and rejects poison pills
func ReserveInventoryActivity(ctx context.Context, req OrderSagaRequest) (string, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("Executing ReserveInventoryActivity", "order_id", req.OrderID, "sku", req.SKU, "qty", req.Quantity, "trace", req.TraceParent)

	// Immediate poison pill check
	if req.TriggerFailure || req.Quantity < 0 || req.Amount < 0 ||
		strings.Contains(strings.ToUpper(req.SKU), "POISON-PILL") {
		_, _ = callInventoryServiceReserve(ctx, req)
		return "", temporal.NewNonRetryableApplicationError(
			"ERR_INVENTORY_REJECTED: poison pill or negative quantity detected",
			"PoisonPillError",
			nil,
		)
	}

	reservationID, err := callInventoryServiceReserve(ctx, req)
	if err != nil {
		return "", temporal.NewNonRetryableApplicationError(
			err.Error(),
			"InventoryError",
			err,
		)
	}
	return reservationID, nil
}

func callInventoryServiceReserve(ctx context.Context, req OrderSagaRequest) (string, error) {
	inventoryURL := fmt.Sprintf("%s/reserve", getInventoryServiceURL())
	payloadBytes, _ := json.Marshal(req)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, inventoryURL, bytes.NewReader(payloadBytes))
	if err != nil {
		return "", err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if req.TraceParent != "" {
		httpReq.Header.Set("traceparent", req.TraceParent)
	}

	resp, err := httpClient.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("inventory service unreachable: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errRes struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(bodyBytes, &errRes) == nil && errRes.Error != "" {
			return "", errors.New(errRes.Error)
		}
		return "", fmt.Errorf("inventory service rejected with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var res struct {
		ReservationID string `json:"reservation_id"`
	}
	_ = json.Unmarshal(bodyBytes, &res)
	if res.ReservationID == "" {
		res.ReservationID = fmt.Sprintf("RES-%s", req.OrderID)
	}
	return res.ReservationID, nil
}
