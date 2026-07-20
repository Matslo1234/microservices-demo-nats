// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	eventsv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/events/v1"
	pb "github.com/GoogleCloudPlatform/microservices-demo/src/frontend/genproto"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const pageViewedSubject = "boutique.evt.storefront.page-viewed.v1"

var (
	errProjectionNotFound    = errors.New("storefront projection not found")
	errProjectionUnavailable = errors.New("storefront projection unavailable")
	errInvalidCurrency       = errors.New("invalid storefront currency")
)

type storefrontQueryRequest struct {
	ProductID    string   `json:"product_id,omitempty"`
	UserID       string   `json:"user_id,omitempty"`
	ProductIDs   []string `json:"product_ids,omitempty"`
	CurrencyCode string   `json:"currency_code,omitempty"`
	OperationID  string   `json:"operation_id,omitempty"`
	OrderID      string   `json:"order_id,omitempty"`
}

type storefrontProductView struct {
	Item  *pb.Product `json:"item"`
	Price *pb.Money   `json:"price"`
}

type storefrontCartItemView struct {
	Item     *pb.Product `json:"item"`
	Quantity int32       `json:"quantity"`
	Price    *pb.Money   `json:"price"`
}

type storefrontQueryResponse struct {
	Products        []storefrontProductView  `json:"products"`
	Product         *storefrontProductView   `json:"product"`
	ProductMeta     []*pb.Product            `json:"product_meta"`
	Items           []storefrontCartItemView `json:"items"`
	Currencies      []string                 `json:"currencies"`
	Recommendations []*pb.Product            `json:"recommendations"`
	Ad              *pb.Ad                   `json:"ad"`
	ShippingCost    *pb.Money                `json:"shipping_cost"`
	ShippingPending bool                     `json:"shipping_pending"`
	CartSize        int                      `json:"cart_size"`
	CartVersion     uint64                   `json:"cart_version"`
	CatalogRevision uint64                   `json:"catalog_revision"`
	RateRevision    uint64                   `json:"rate_revision"`
	UpdatedAt       time.Time                `json:"updated_at"`
	Stale           []string                 `json:"stale"`
	Operation       *cartOperation           `json:"operation"`
	Order           *orderStatus             `json:"order"`
	Error           string                   `json:"error"`
}

type natsHeaderCarrier nats.Header

func (carrier natsHeaderCarrier) Get(key string) string { return nats.Header(carrier).Get(key) }
func (carrier natsHeaderCarrier) Set(key, value string) { nats.Header(carrier).Set(key, value) }
func (carrier natsHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(carrier))
	for key := range carrier {
		keys = append(keys, key)
	}
	return keys
}

func connectFrontendNATS() (*nats.Conn, nats.JetStreamContext, time.Duration, time.Duration, error) {
	url, user, password, caFile := os.Getenv("NATS_URL"), os.Getenv("NATS_USER"), os.Getenv("NATS_PASSWORD"), os.Getenv("NATS_CA_FILE")
	if url == "" || user == "" || password == "" || caFile == "" {
		return nil, nil, 0, 0, fmt.Errorf("NATS_URL, NATS_USER, NATS_PASSWORD, and NATS_CA_FILE are required")
	}
	connectTimeout, err := durationEnv("NATS_CONNECT_TIMEOUT", 2*time.Second)
	if err != nil {
		return nil, nil, 0, 0, err
	}
	reconnectWait, err := durationEnv("NATS_RECONNECT_WAIT", 2*time.Second)
	if err != nil {
		return nil, nil, 0, 0, err
	}
	requestTimeout, err := durationEnv("NATS_REQUEST_TIMEOUT", 2*time.Second)
	if err != nil {
		return nil, nil, 0, 0, err
	}
	publishTimeout, err := durationEnv("NATS_PUBLISH_TIMEOUT", 5*time.Second)
	if err != nil {
		return nil, nil, 0, 0, err
	}
	pingInterval, err := durationEnv("NATS_PING_INTERVAL", 20*time.Second)
	if err != nil {
		return nil, nil, 0, 0, err
	}
	maxReconnects, err := intEnv("NATS_MAX_RECONNECTS", -1)
	if err != nil {
		return nil, nil, 0, 0, err
	}
	maxPings, err := intEnv("NATS_MAX_PINGS_OUT", 2)
	if err != nil {
		return nil, nil, 0, 0, err
	}
	nc, err := nats.Connect(url,
		nats.Name("frontend/phase3"), nats.UserInfo(user, password), nats.RootCAs(caFile),
		nats.Timeout(connectTimeout), nats.ReconnectWait(reconnectWait), nats.MaxReconnects(maxReconnects),
		nats.PingInterval(pingInterval), nats.MaxPingsOutstanding(maxPings),
	)
	if err != nil {
		return nil, nil, 0, 0, fmt.Errorf("connect frontend to NATS: %w", err)
	}
	js, err := nc.JetStream(nats.PublishAsyncMaxPending(128))
	if err != nil {
		nc.Close()
		return nil, nil, 0, 0, fmt.Errorf("create frontend JetStream context: %w", err)
	}
	return nc, js, requestTimeout, publishTimeout, nil
}

func durationEnv(name string, fallback time.Duration) (time.Duration, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", name, err)
	}
	return parsed, nil
}

func intEnv(name string, fallback int) (int, error) {
	value := os.Getenv(name)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", name, err)
	}
	return parsed, nil
}

func (fe *frontendServer) storefrontQuery(ctx context.Context, view string, request storefrontQueryRequest) (*storefrontQueryResponse, error) {
	encoded, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	queryContext, cancel := context.WithTimeout(ctx, fe.natsRequestTimeout)
	defer cancel()
	message, err := fe.natsConn.RequestWithContext(queryContext, "boutique.qry.storefront."+view+".v1", encoded)
	if err != nil {
		if errors.Is(err, nats.ErrNoResponders) || errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("%w: %v", errProjectionUnavailable, err)
		}
		return nil, err
	}
	var response storefrontQueryResponse
	if err := json.Unmarshal(message.Data, &response); err != nil {
		return nil, fmt.Errorf("decode storefront response: %w", err)
	}
	switch response.Error {
	case "":
		return &response, nil
	case "NOT_FOUND":
		return nil, errProjectionNotFound
	case "INVALID_CURRENCY":
		return nil, errInvalidCurrency
	default:
		return nil, fmt.Errorf("%w: %s", errProjectionUnavailable, response.Error)
	}
}

func filterCurrencies(currencies []string) []string {
	filtered := make([]string, 0, len(currencies))
	for _, currency := range currencies {
		if whitelistedCurrencies[currency] {
			filtered = append(filtered, currency)
		}
	}
	return filtered
}

func (fe *frontendServer) publishPageView(ctx context.Context, session, pageType, productID string, categories []string, cartVersion uint64) error {
	now := time.Now().UTC()
	messageID := uuid.NewString()
	version := uint64(now.UnixNano())
	payload := &eventsv1.StorefrontPageViewedEvent{
		SessionId: session, PageType: pageType, ProductId: productID,
		CategoryIds: append([]string(nil), categories...), CartVersion: cartVersion,
	}
	wrapped, err := anypb.New(payload)
	if err != nil {
		return err
	}
	envelope := &commonv1.MessageEnvelope{
		MessageId: messageID, MessageType: "boutique.storefront.PageViewed.v1", SchemaVersion: 1,
		OccurredAt: timestamppb.New(now), Producer: "frontend/phase3",
		AggregateType: "storefront-session", AggregateId: session, AggregateVersion: version,
		CorrelationId: requestID(ctx), Data: wrapped,
	}
	headers := nats.Header{}
	otel.GetTextMapPropagator().Inject(ctx, natsHeaderCarrier(headers))
	envelope.Traceparent = headers.Get("traceparent")
	envelope.Tracestate = headers.Get("tracestate")
	encoded, err := proto.Marshal(envelope)
	if err != nil {
		return err
	}
	publishContext, cancel := context.WithTimeout(context.Background(), fe.natsPublishTimeout)
	defer cancel()
	message := &nats.Msg{Subject: pageViewedSubject, Data: encoded, Header: nats.Header{}}
	message.Header.Set("Nats-Msg-Id", messageID)
	message.Header.Set("Content-Type", "application/protobuf")
	_, err = fe.natsJS.PublishMsg(message, nats.Context(publishContext), nats.MsgId(messageID))
	return err
}

func requestID(ctx context.Context) string {
	if value, ok := ctx.Value(ctxKeyRequestID{}).(string); ok {
		return value
	}
	return uuid.NewString()
}
