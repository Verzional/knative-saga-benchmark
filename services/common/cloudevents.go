package common

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// CloudEvent represents a standard CNCF CloudEvent v1.0 envelope with distributed tracing extensions
type CloudEvent struct {
	SpecVersion     string          `json:"specversion"`
	ID              string          `json:"id"`
	Source          string          `json:"source"`
	Type            string          `json:"type"`
	DataContentType string          `json:"datacontenttype"`
	Time            time.Time       `json:"time"`
	TraceParent     string          `json:"traceparent,omitempty"`
	IdempotencyKey  string          `json:"idempotencykey,omitempty"`
	Data            json.RawMessage `json:"data"`
}

// OrderPayload represents the standard order transaction data
type OrderPayload struct {
	OrderID         string  `json:"order_id"`
	CustomerID      string  `json:"customer_id"`
	SKU             string  `json:"sku"`
	Quantity        int     `json:"quantity"`
	Amount          float64 `json:"amount"`
	PaymentMethod   string  `json:"payment_method"`
	Status          string  `json:"status"` // PENDING, CONFIRMED, CANCELLED, COMPENSATED
	FailReason      string  `json:"fail_reason,omitempty"`
	StartTimestamp  int64   `json:"start_timestamp_ns"`
	PaymentDuration int64   `json:"payment_duration_ns,omitempty"`
	InventoryDuration int64 `json:"inventory_duration_ns,omitempty"`
	CompDuration    int64   `json:"compensation_duration_ns,omitempty"`
}

// NewCloudEvent creates a new CNCF-compliant CloudEvent with tracing & idempotency support
func NewCloudEvent(eventType, source string, payload interface{}, traceparent, idempotencyKey string) (*CloudEvent, error) {
	dataBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	if idempotencyKey == "" {
		idempotencyKey = uuid.New().String()
	}
	return &CloudEvent{
		SpecVersion:     "1.0",
		ID:              uuid.New().String(),
		Source:          source,
		Type:            eventType,
		DataContentType: "application/json",
		Time:            time.Now().UTC(),
		TraceParent:     traceparent,
		IdempotencyKey:  idempotencyKey,
		Data:            dataBytes,
	}, nil
}
