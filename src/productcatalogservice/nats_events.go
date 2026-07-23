// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"

	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	eventsv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/events/v1"
	pb "github.com/GoogleCloudPlatform/microservices-demo/src/productcatalogservice/genproto"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	catalogProductSubject  = "boutique.evt.catalog.product-upserted.v1"
	catalogProductType     = "boutique.catalog.ProductUpserted.v1"
	catalogSnapshotSubject = "boutique.evt.catalog.snapshot-completed.v1"
	catalogSnapshotType    = "boutique.catalog.SnapshotCompleted.v1"
)

type catalogEventPublisher struct {
	nc             *nats.Conn
	js             nats.JetStreamContext
	publishTimeout time.Duration
}

type catalogIdentity struct {
	revision uint64
	checksum string
	products []*commonv1.ProductSnapshot
}

func natsIsRequired() bool {
	required, err := strconv.ParseBool(os.Getenv("NATS_REQUIRED"))
	return err == nil && required
}

func durationFromEnv(name string, fallback time.Duration) (time.Duration, error) {
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

func connectCatalogPublisher() (*catalogEventPublisher, error) {
	url := os.Getenv("NATS_URL")
	user := os.Getenv("NATS_USER")
	password := os.Getenv("NATS_PASSWORD")
	caFile := os.Getenv("NATS_CA_FILE")
	if url == "" || user == "" || password == "" || caFile == "" {
		return nil, fmt.Errorf("NATS_URL, NATS_USER, NATS_PASSWORD, and NATS_CA_FILE are required")
	}

	connectTimeout, err := durationFromEnv("NATS_CONNECT_TIMEOUT", 2*time.Second)
	if err != nil {
		return nil, err
	}
	reconnectWait, err := durationFromEnv("NATS_RECONNECT_WAIT", 2*time.Second)
	if err != nil {
		return nil, err
	}
	pingInterval, err := durationFromEnv("NATS_PING_INTERVAL", 20*time.Second)
	if err != nil {
		return nil, err
	}
	publishTimeout, err := durationFromEnv("NATS_PUBLISH_TIMEOUT", 5*time.Second)
	if err != nil {
		return nil, err
	}
	maxReconnects := -1
	if value := os.Getenv("NATS_MAX_RECONNECTS"); value != "" {
		maxReconnects, err = strconv.Atoi(value)
		if err != nil {
			return nil, fmt.Errorf("invalid NATS_MAX_RECONNECTS: %w", err)
		}
	}
	maxPingsOut := 2
	if value := os.Getenv("NATS_MAX_PINGS_OUT"); value != "" {
		maxPingsOut, err = strconv.Atoi(value)
		if err != nil {
			return nil, fmt.Errorf("invalid NATS_MAX_PINGS_OUT: %w", err)
		}
	}

	nc, err := nats.Connect(
		url,
		nats.Name("productcatalogservice/phase2"),
		nats.UserInfo(user, password),
		nats.RootCAs(caFile),
		nats.Timeout(connectTimeout),
		nats.ReconnectWait(reconnectWait),
		nats.MaxReconnects(maxReconnects),
		nats.PingInterval(pingInterval),
		nats.MaxPingsOutstanding(maxPingsOut),
		nats.DisconnectErrHandler(func(_ *nats.Conn, disconnectErr error) {
			log.Warnf("NATS disconnected: %v", disconnectErr)
		}),
		nats.ReconnectHandler(func(conn *nats.Conn) {
			log.Infof("NATS reconnected to %s", conn.ConnectedUrlRedacted())
		}),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, asyncErr error) {
			log.Errorf("NATS asynchronous error: %v", asyncErr)
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("connect to NATS: %w", err)
	}
	js, err := nc.JetStream(nats.PublishAsyncMaxPending(256))
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("create JetStream context: %w", err)
	}
	return &catalogEventPublisher{nc: nc, js: js, publishTimeout: publishTimeout}, nil
}

func identifyCatalog(products []*pb.Product) (*catalogIdentity, error) {
	sorted := append([]*pb.Product(nil), products...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Id < sorted[j].Id })

	hasher := sha256.New()
	snapshots := make([]*commonv1.ProductSnapshot, 0, len(sorted))
	for _, product := range sorted {
		productSnapshot := &commonv1.ProductSnapshot{
			ProductId:   product.Id,
			Name:        product.Name,
			Description: product.Description,
			Picture:     product.Picture,
			PriceUsd: &commonv1.Money{
				CurrencyCode: product.PriceUsd.CurrencyCode,
				Units:        product.PriceUsd.Units,
				Nanos:        product.PriceUsd.Nanos,
			},
			Categories: append([]string(nil), product.Categories...),
		}
		withoutVersion, err := proto.MarshalOptions{Deterministic: true}.Marshal(productSnapshot)
		if err != nil {
			return nil, fmt.Errorf("marshal product %s: %w", product.Id, err)
		}
		productSnapshot.ProductVersion = stableVersion(withoutVersion)
		encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(productSnapshot)
		if err != nil {
			return nil, fmt.Errorf("marshal versioned product %s: %w", product.Id, err)
		}
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(encoded)))
		hasher.Write(length[:])
		hasher.Write(encoded)
		snapshots = append(snapshots, productSnapshot)
	}
	digest := hasher.Sum(nil)
	return &catalogIdentity{
		revision: stableVersion(digest),
		checksum: hex.EncodeToString(digest),
		products: snapshots,
	}, nil
}

func stableVersion(data []byte) uint64 {
	digest := sha256.Sum256(data)
	version := binary.BigEndian.Uint64(digest[:8]) & 0x7fffffffffffffff
	if version == 0 {
		return 1
	}
	return version
}

func deterministicMessageID(parts ...string) string {
	hasher := sha256.New()
	for _, part := range parts {
		hasher.Write([]byte(part))
		hasher.Write([]byte{0})
	}
	id := hasher.Sum(nil)[:16]
	id[6] = (id[6] & 0x0f) | 0x50
	id[8] = (id[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		id[0:4], id[4:6], id[6:8], id[8:10], id[10:16])
}

func (p *catalogEventPublisher) publish(subject string, envelope *commonv1.MessageEnvelope) error {
	encoded, err := proto.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal %s envelope: %w", subject, err)
	}
	message := &nats.Msg{Subject: subject, Data: encoded, Header: nats.Header{}}
	message.Header.Set("Nats-Msg-Id", envelope.MessageId)
	if envelope.Traceparent != "" {
		message.Header.Set("traceparent", envelope.Traceparent)
	}
	if envelope.Tracestate != "" {
		message.Header.Set("tracestate", envelope.Tracestate)
	}

	var publishErr error
	for attempt := 1; attempt <= 3; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), p.publishTimeout)
		_, publishErr = p.js.PublishMsg(message, nats.Context(ctx))
		cancel()
		if publishErr == nil {
			log.WithFields(logrus.Fields{
				"topic":          subject,
				"message_kind":   "event",
				"message_id":     envelope.MessageId,
				"correlation_id": correlationID(envelope.CorrelationId),
			}).Debug("NATS event sent")
			return nil
		}
		log.WithFields(logrus.Fields{
			"topic":          subject,
			"message_id":     envelope.MessageId,
			"correlation_id": correlationID(envelope.CorrelationId),
		}).Warnf("JetStream publish attempt %d failed: %v", attempt, publishErr)
	}
	return fmt.Errorf("publish %s after retries: %w", subject, publishErr)
}

func (p *catalogEventPublisher) publishBootstrap(products []*pb.Product) error {
	identity, err := identifyCatalog(products)
	if err != nil {
		return err
	}
	occurredAt := timestamppb.Now()
	revisionText := strconv.FormatUint(identity.revision, 10)
	correlationID := deterministicMessageID("catalog-bootstrap", revisionText)
	for _, product := range identity.products {
		payload := &eventsv1.CatalogProductUpsertedEvent{
			Product:         product,
			CatalogRevision: identity.revision,
		}
		packed, err := anypb.New(payload)
		if err != nil {
			return fmt.Errorf("pack product %s: %w", product.ProductId, err)
		}
		envelope := &commonv1.MessageEnvelope{
			MessageId:        deterministicMessageID(catalogProductSubject, revisionText, product.ProductId),
			MessageType:      catalogProductType,
			SchemaVersion:    1,
			OccurredAt:       occurredAt,
			Producer:         "productcatalogservice/phase2",
			AggregateType:    "product",
			AggregateId:      product.ProductId,
			AggregateVersion: product.ProductVersion,
			CorrelationId:    correlationID,
			Data:             packed,
		}
		if err := p.publish(catalogProductSubject, envelope); err != nil {
			return err
		}
	}

	payload := &eventsv1.CatalogSnapshotCompletedEvent{
		CatalogRevision: identity.revision,
		ProductCount:    uint32(len(identity.products)),
		Checksum:        identity.checksum,
	}
	packed, err := anypb.New(payload)
	if err != nil {
		return fmt.Errorf("pack catalog snapshot: %w", err)
	}
	envelope := &commonv1.MessageEnvelope{
		MessageId:        deterministicMessageID(catalogSnapshotSubject, revisionText, identity.checksum),
		MessageType:      catalogSnapshotType,
		SchemaVersion:    1,
		OccurredAt:       occurredAt,
		Producer:         "productcatalogservice/phase2",
		AggregateType:    "catalog",
		AggregateId:      "catalog",
		AggregateVersion: identity.revision,
		CorrelationId:    correlationID,
		Data:             packed,
	}
	if err := p.publish(catalogSnapshotSubject, envelope); err != nil {
		return err
	}
	return nil
}

func correlationID(value string) string {
	if value == "" {
		return "unknown"
	}
	return value
}

func (p *catalogEventPublisher) drain() {
	if p == nil || p.nc == nil {
		return
	}
	if err := p.nc.Drain(); err != nil {
		log.Warnf("failed to drain NATS connection: %v", err)
	}
}
