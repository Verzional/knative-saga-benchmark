package temporal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

// OrderSagaRequest input for the workflow
type OrderSagaRequest struct {
	OrderID        string  `json:"order_id"`
	CustomerID     string  `json:"customer_id"`
	SKU            string  `json:"sku"`
	Quantity       int     `json:"quantity"`
	Amount         float64 `json:"amount"`
	PaymentMethod  string  `json:"payment_method"`
	TriggerFailure bool    `json:"trigger_failure"`
}

// OrderSagaResult output of the workflow
type OrderSagaResult struct {
	OrderID      string    `json:"order_id"`
	Status       string    `json:"status"` // "COMPLETED" or "COMPENSATED"
	FailureStep  string    `json:"failure_step,omitempty"`
	ErrorMessage string    `json:"error_message,omitempty"`
	FinishedAt   time.Time `json:"finished_at"`
}

// CreateOrderSaga orchestrates the distributed saga with backward compensation
func CreateOrderSaga(ctx workflow.Context, req OrderSagaRequest) (*OrderSagaResult, error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	logger := workflow.GetLogger(ctx)
	logger.Info("Starting CreateOrderSaga", "order_id", req.OrderID)

	var compensations []func(workflow.Context)

	// Step 1: Process Payment
	var paymentRef string
	err := workflow.ExecuteActivity(ctx, ProcessPaymentActivity, req).Get(ctx, &paymentRef)
	if err != nil {
		return &OrderSagaResult{
			OrderID:      req.OrderID,
			Status:       "FAILED",
			FailureStep:  "Payment",
			ErrorMessage: err.Error(),
			FinishedAt:   workflow.Now(ctx),
		}, err
	}

	// Register Payment Compensation
	compensations = append(compensations, func(compCtx workflow.Context) {
		_ = workflow.ExecuteActivity(compCtx, RefundPaymentActivity, req.OrderID, paymentRef).Get(compCtx, nil)
	})

	// Step 2: Reserve Inventory
	var invReservationID string
	err = workflow.ExecuteActivity(ctx, ReserveInventoryActivity, req).Get(ctx, &invReservationID)
	if err != nil {
		logger.Error("Inventory reservation failed, triggering compensation!", "error", err)

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

// ProcessPaymentActivity simulates payment processing
func ProcessPaymentActivity(ctx context.Context, req OrderSagaRequest) (string, error) {
	activity.GetLogger(ctx).Info("Processing payment", "amount", req.Amount)
	return fmt.Sprintf("PAY-%s", req.OrderID), nil
}

// RefundPaymentActivity performs compensatory refund
func RefundPaymentActivity(ctx context.Context, orderID, paymentRef string) error {
	activity.GetLogger(ctx).Info("Executing Compensatory Refund", "orderID", orderID, "ref", paymentRef)
	return nil
}

// ReserveInventoryActivity simulates stock reservation and rejects poison pills
func ReserveInventoryActivity(ctx context.Context, req OrderSagaRequest) (string, error) {
	if req.TriggerFailure || req.Quantity < 0 || req.Amount < 0 {
		return "", errors.New("ERR_INVENTORY_REJECTED: poison pill or negative quantity detected")
	}
	activity.GetLogger(ctx).Info("Reserving inventory", "sku", req.SKU, "qty", req.Quantity)
	return fmt.Sprintf("RES-%s", req.OrderID), nil
}

// ReleaseInventoryActivity compensates reserved stock
func ReleaseInventoryActivity(ctx context.Context, orderID string) error {
	activity.GetLogger(ctx).Info("Releasing inventory compensation", "orderID", orderID)
	return nil
}
