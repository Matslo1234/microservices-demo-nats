package storefront

import (
	"testing"

	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
)

func TestConvertMatchesCurrencyServiceAlgorithm(t *testing.T) {
	view := CurrencyView{Rates: []Rate{
		{CurrencyCode: "EUR", UnitsPerBase: 1},
		{CurrencyCode: "USD", UnitsPerBase: 1.1305},
		{CurrencyCode: "JPY", UnitsPerBase: 126.40},
	}}
	result := view.Convert(&commonv1.Money{CurrencyCode: "USD", Units: 19, Nanos: 990_000_000}, "JPY")
	if result == nil || result.CurrencyCode != "JPY" || result.Units != 2235 || result.Nanos != 60_592_707 {
		t.Fatalf("unexpected conversion: %#v", result)
	}
}
