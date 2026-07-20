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

package storefront

import (
	"math"
	"sort"
	"time"

	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
)

const (
	CatalogKey  = "catalog"
	CurrencyKey = "currency"
)

type ProductView struct {
	Product         *commonv1.ProductSnapshot `json:"product"`
	CatalogRevision uint64                    `json:"catalog_revision"`
	Removed         bool                      `json:"removed,omitempty"`
	UpdatedAt       time.Time                 `json:"updated_at"`
}

type CatalogView struct {
	CatalogRevision uint64    `json:"catalog_revision"`
	ProductCount    uint32    `json:"product_count"`
	Checksum        string    `json:"checksum"`
	UpdatedAt       time.Time `json:"updated_at"`
}

type Rate struct {
	CurrencyCode string  `json:"currency_code"`
	UnitsPerBase float64 `json:"units_per_base"`
}

type CurrencyView struct {
	BaseCurrencyCode string    `json:"base_currency_code"`
	Rates            []Rate    `json:"rates"`
	EffectiveSeconds int64     `json:"effective_seconds"`
	EffectiveNanos   int32     `json:"effective_nanos"`
	RateRevision     uint64    `json:"rate_revision"`
	UpdatedAt        time.Time `json:"updated_at"`
}

type CartView struct {
	Cart      *commonv1.CartSnapshot `json:"cart"`
	UpdatedAt time.Time              `json:"updated_at"`
}

type RecommendationView struct {
	SessionID      string    `json:"session_id"`
	ContextVersion uint64    `json:"context_version"`
	ProductIDs     []string  `json:"product_ids,omitempty"`
	ExpiresAt      time.Time `json:"expires_at"`
	FailureCode    string    `json:"failure_code,omitempty"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type Ad struct {
	RedirectURL string `json:"redirect_url"`
	Text        string `json:"text"`
}

type AdView struct {
	SessionID      string    `json:"session_id"`
	PageType       string    `json:"page_type"`
	ContextVersion uint64    `json:"context_version"`
	Ads            []Ad      `json:"ads,omitempty"`
	ExpiresAt      time.Time `json:"expires_at"`
	FailureCode    string    `json:"failure_code,omitempty"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type CartQuoteView struct {
	UserID      string          `json:"user_id"`
	CartVersion uint64          `json:"cart_version"`
	CostUSD     *commonv1.Money `json:"cost_usd,omitempty"`
	ExpiresAt   time.Time       `json:"expires_at"`
	FailureCode string          `json:"failure_code,omitempty"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

type OperationView struct {
	OperationID string    `json:"operation_id"`
	CommandID   string    `json:"command_id"`
	Kind        string    `json:"kind"`
	Status      string    `json:"status"`
	UserID      string    `json:"user_id"`
	FailureCode string    `json:"failure_code,omitempty"`
	Retryable   bool      `json:"retryable,omitempty"`
	SafeMessage string    `json:"safe_message,omitempty"`
	CartVersion uint64    `json:"cart_version,omitempty"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type OrderView struct {
	OrderID            string                           `json:"order_id"`
	UserID             string                           `json:"user_id"`
	Status             string                           `json:"status"`
	Stage              string                           `json:"stage,omitempty"`
	Snapshot           *commonv1.SanitizedOrderSnapshot `json:"snapshot,omitempty"`
	FailureCode        string                           `json:"failure_code,omitempty"`
	Retryable          bool                             `json:"retryable,omitempty"`
	SafeMessage        string                           `json:"safe_message,omitempty"`
	NotificationStatus string                           `json:"notification_status,omitempty"`
	AggregateVersion   uint64                           `json:"aggregate_version"`
	UpdatedAt          time.Time                        `json:"updated_at"`
}

func ProductKey(productID string) string { return "product." + productID }

func RecommendationKey(sessionID string) string { return "recommendation." + sessionID }

func AdKey(sessionID string) string { return "ad." + sessionID }

func CartQuoteKey(userID string) string { return "quote." + userID }

func OperationKey(operationID string) string { return "operation." + operationID }

func OrderKey(orderID string) string { return "order." + orderID }

func (view CurrencyView) SupportedCurrencies() []string {
	codes := make([]string, 0, len(view.Rates))
	for _, rate := range view.Rates {
		codes = append(codes, rate.CurrencyCode)
	}
	sort.Strings(codes)
	return codes
}

func (view CurrencyView) Convert(from *commonv1.Money, toCode string) *commonv1.Money {
	rates := make(map[string]float64, len(view.Rates))
	for _, rate := range view.Rates {
		rates[rate.CurrencyCode] = rate.UnitsPerBase
	}
	fromRate, fromOK := rates[from.CurrencyCode]
	toRate, toOK := rates[toCode]
	if !fromOK || !toOK || fromRate == 0 {
		return nil
	}

	euroUnits, euroNanos := carry(float64(from.Units)/fromRate, float64(from.Nanos)/fromRate)
	euroNanos = math.Round(euroNanos)
	resultUnits, resultNanos := carry(euroUnits*toRate, euroNanos*toRate)
	return &commonv1.Money{
		CurrencyCode: toCode,
		Units:        int64(math.Floor(resultUnits)),
		Nanos:        int32(math.Floor(resultNanos)),
	}
}

func carry(units, nanos float64) (float64, float64) {
	const fractionSize = 1_000_000_000
	nanos += math.Mod(units, 1) * fractionSize
	units = math.Floor(units) + math.Floor(nanos/fractionSize)
	nanos = math.Mod(nanos, fractionSize)
	return units, nanos
}
