// Copyright 2018 Google LLC
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
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	commonv1 "github.com/GoogleCloudPlatform/microservices-demo/protos/common/v1"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	pb "github.com/GoogleCloudPlatform/microservices-demo/src/frontend/genproto"
	"github.com/GoogleCloudPlatform/microservices-demo/src/frontend/money"
	"github.com/GoogleCloudPlatform/microservices-demo/src/frontend/validator"
)

type platformDetails struct {
	css      string
	provider string
}

var (
	frontendMessage  = strings.TrimSpace(os.Getenv("FRONTEND_MESSAGE"))
	isCymbalBrand    = "true" == strings.ToLower(os.Getenv("CYMBAL_BRANDING"))
	assistantEnabled = "true" == strings.ToLower(os.Getenv("ENABLE_ASSISTANT"))
	templates        = template.Must(template.New("").
				Funcs(template.FuncMap{
			"renderMoney":        renderMoney,
			"renderCurrencyLogo": renderCurrencyLogo,
		}).ParseGlob("templates/*.html"))
	plat platformDetails
)

var validEnvs = []string{"local", "gcp", "azure", "aws", "onprem", "alibaba"}

func (fe *frontendServer) homeHandler(w http.ResponseWriter, r *http.Request) {
	log := r.Context().Value(ctxKeyLog{}).(logrus.FieldLogger)
	log.WithField("currency", currentCurrency(r)).Info("home")
	view, err := fe.storefrontQuery(r.Context(), "home", storefrontQueryRequest{
		UserID: sessionID(r), CurrencyCode: currentCurrency(r),
	})
	if err != nil {
		renderStorefrontError(log, r, w, errors.Wrap(err, "could not retrieve home view"))
		return
	}
	defer func() {
		if err := fe.publishPageView(r.Context(), sessionID(r), "home", "", nil, view.CartVersion); err != nil {
			log.WithError(err).Warn("failed to publish home page view")
		}
	}()

	// Set ENV_PLATFORM (default to local if not set; use env var if set; otherwise detect GCP, which overrides env)_
	var env = os.Getenv("ENV_PLATFORM")
	// Only override from env variable if set + valid env
	if env == "" || stringinSlice(validEnvs, env) == false {
		fmt.Println("env platform is either empty or invalid")
		env = "local"
	}
	// Autodetect GCP
	addrs, err := net.LookupHost("metadata.google.internal.")
	if err == nil && len(addrs) >= 0 {
		log.Debugf("Detected Google metadata server: %v, setting ENV_PLATFORM to GCP.", addrs)
		env = "gcp"
	}

	log.Debugf("ENV_PLATFORM is: %s", env)
	plat = platformDetails{}
	plat.setPlatformDetails(strings.ToLower(env))

	if err := templates.ExecuteTemplate(w, "home", injectCommonTemplateData(r, map[string]interface{}{
		"show_currency": true,
		"currencies":    filterCurrencies(view.Currencies),
		"products":      view.Products,
		"cart_size":     view.CartSize,
		"banner_color":  os.Getenv("BANNER_COLOR"), // illustrates canary deployments
		"ad":            view.Ad,
	})); err != nil {
		log.Error(err)
	}
}

func (plat *platformDetails) setPlatformDetails(env string) {
	if env == "aws" {
		plat.provider = "AWS"
		plat.css = "aws-platform"
	} else if env == "onprem" {
		plat.provider = "On-Premises"
		plat.css = "onprem-platform"
	} else if env == "azure" {
		plat.provider = "Azure"
		plat.css = "azure-platform"
	} else if env == "gcp" {
		plat.provider = "Google Cloud"
		plat.css = "gcp-platform"
	} else if env == "alibaba" {
		plat.provider = "Alibaba Cloud"
		plat.css = "alibaba-platform"
	} else {
		plat.provider = "local"
		plat.css = "local"
	}
}

func (fe *frontendServer) productHandler(w http.ResponseWriter, r *http.Request) {
	log := r.Context().Value(ctxKeyLog{}).(logrus.FieldLogger)
	id := mux.Vars(r)["id"]
	if id == "" {
		renderHTTPError(log, r, w, errors.New("product id not specified"), http.StatusBadRequest)
		return
	}
	log.WithField("id", id).WithField("currency", currentCurrency(r)).
		Debug("serving product page")

	view, err := fe.storefrontQuery(r.Context(), "product", storefrontQueryRequest{
		ProductID: id, UserID: sessionID(r), CurrencyCode: currentCurrency(r),
	})
	if err != nil {
		renderStorefrontError(log, r, w, errors.Wrap(err, "could not retrieve product view"))
		return
	}
	if view.Product == nil || view.Product.Item == nil {
		renderStorefrontError(log, r, w, errors.New("product projection is incomplete"))
		return
	}
	defer func() {
		if err := fe.publishPageView(r.Context(), sessionID(r), "product", id, view.Product.Item.Categories, view.CartVersion); err != nil {
			log.WithError(err).Warn("failed to publish product page view")
		}
	}()

	if err := templates.ExecuteTemplate(w, "product", injectCommonTemplateData(r, map[string]interface{}{
		"ad":              view.Ad,
		"show_currency":   true,
		"currencies":      filterCurrencies(view.Currencies),
		"product":         view.Product,
		"recommendations": view.Recommendations,
		"cart_size":       view.CartSize,
		"packagingInfo":   nil,
	})); err != nil {
		log.Println(err)
	}
}

func (fe *frontendServer) addToCartHandler(w http.ResponseWriter, r *http.Request) {
	log := r.Context().Value(ctxKeyLog{}).(logrus.FieldLogger)
	quantity, _ := strconv.ParseUint(r.FormValue("quantity"), 10, 32)
	productID := r.FormValue("product_id")
	payload := validator.AddToCartPayload{
		Quantity:  quantity,
		ProductID: productID,
	}
	if err := payload.Validate(); err != nil {
		renderHTTPError(log, r, w, validator.ValidationErrorResponse(err), http.StatusUnprocessableEntity)
		return
	}
	log.WithField("product", payload.ProductID).WithField("quantity", payload.Quantity).Debug("adding to cart")

	view, err := fe.storefrontQuery(r.Context(), "product", storefrontQueryRequest{
		ProductID: payload.ProductID, UserID: sessionID(r), CurrencyCode: currentCurrency(r),
	})
	if err != nil {
		renderStorefrontError(log, r, w, errors.Wrap(err, "could not validate product"))
		return
	}
	if view.Product == nil || view.Product.Item == nil {
		renderStorefrontError(log, r, w, errors.New("product projection is incomplete"))
		return
	}

	operationID, err := cartOperationID(r, sessionID(r), "add-item")
	if err != nil {
		renderHTTPError(log, r, w, err, http.StatusBadRequest)
		return
	}
	setOperationHeaders(w, operationID)
	if err := fe.publishCartAdd(r.Context(), operationID, sessionID(r), view.Product.Item.GetId(),
		int32(payload.Quantity), view.CartVersion); err != nil {
		w.Header().Set("Retry-After", "1")
		renderHTTPError(log, r, w, errors.Wrap(err, "failed to queue cart update"), http.StatusServiceUnavailable)
		return
	}
	operation, err := fe.waitForCartOperation(r.Context(), operationID, sessionID(r))
	if err != nil {
		writeAcceptedOperation(w, r, operationID, "cart.add-item", baseUrl+"/cart")
		return
	}
	if operation.Status == "REJECTED" {
		writeRejectedOperation(w, r, operation, baseUrl+"/cart")
		return
	}
	w.Header().Set("location", baseUrl+"/cart")
	w.WriteHeader(http.StatusFound)
}

func (fe *frontendServer) emptyCartHandler(w http.ResponseWriter, r *http.Request) {
	log := r.Context().Value(ctxKeyLog{}).(logrus.FieldLogger)
	log.Debug("emptying cart")

	view, err := fe.storefrontQuery(r.Context(), "cart", storefrontQueryRequest{
		UserID: sessionID(r), CurrencyCode: currentCurrency(r),
	})
	if err != nil {
		renderStorefrontError(log, r, w, errors.Wrap(err, "could not retrieve cart version"))
		return
	}
	operationID, err := cartOperationID(r, sessionID(r), "clear")
	if err != nil {
		renderHTTPError(log, r, w, err, http.StatusBadRequest)
		return
	}
	setOperationHeaders(w, operationID)
	if err := fe.publishCartClear(r.Context(), operationID, sessionID(r), view.CartVersion); err != nil {
		w.Header().Set("Retry-After", "1")
		renderHTTPError(log, r, w, errors.Wrap(err, "failed to queue cart clear"), http.StatusServiceUnavailable)
		return
	}
	operation, err := fe.waitForCartOperation(r.Context(), operationID, sessionID(r))
	if err != nil {
		writeAcceptedOperation(w, r, operationID, "cart.clear", baseUrl+"/")
		return
	}
	if operation.Status == "REJECTED" {
		writeRejectedOperation(w, r, operation, baseUrl+"/")
		return
	}
	w.Header().Set("location", baseUrl+"/")
	w.WriteHeader(http.StatusFound)
}

func (fe *frontendServer) viewCartHandler(w http.ResponseWriter, r *http.Request) {
	log := r.Context().Value(ctxKeyLog{}).(logrus.FieldLogger)
	log.Debug("view user cart")
	view, err := fe.storefrontQuery(r.Context(), "cart", storefrontQueryRequest{
		UserID: sessionID(r), CurrencyCode: currentCurrency(r),
	})
	if err != nil {
		renderStorefrontError(log, r, w, errors.Wrap(err, "could not retrieve cart view"))
		return
	}
	defer func() {
		if err := fe.publishPageView(r.Context(), sessionID(r), "cart", "", nil, view.CartVersion); err != nil {
			log.WithError(err).Warn("failed to publish cart page view")
		}
	}()
	totalPrice := pb.Money{CurrencyCode: currentCurrency(r)}
	for _, item := range view.Items {
		totalPrice = money.Must(money.Sum(totalPrice, *item.Price))
	}
	shippingCost := view.ShippingCost
	if shippingCost == nil {
		shippingCost = &pb.Money{CurrencyCode: currentCurrency(r)}
	} else {
		totalPrice = money.Must(money.Sum(totalPrice, *shippingCost))
	}
	year := time.Now().Year()

	if err := templates.ExecuteTemplate(w, "cart", injectCommonTemplateData(r, map[string]interface{}{
		"currencies":       filterCurrencies(view.Currencies),
		"recommendations":  view.Recommendations,
		"cart_size":        view.CartSize,
		"shipping_cost":    shippingCost,
		"shipping_pending": view.ShippingPending,
		"show_currency":    true,
		"total_cost":       totalPrice,
		"items":            view.Items,
		"expiration_years": []int{year, year + 1, year + 2, year + 3, year + 4},
	})); err != nil {
		log.Println(err)
	}
}

func (fe *frontendServer) placeOrderHandler(w http.ResponseWriter, r *http.Request) {
	log := r.Context().Value(ctxKeyLog{}).(logrus.FieldLogger)
	log.Debug("placing order")

	var (
		email         = r.FormValue("email")
		streetAddress = r.FormValue("street_address")
		zipCode, _    = strconv.ParseInt(r.FormValue("zip_code"), 10, 32)
		city          = r.FormValue("city")
		state         = r.FormValue("state")
		country       = r.FormValue("country")
		ccNumber      = r.FormValue("credit_card_number")
		ccMonth, _    = strconv.ParseInt(r.FormValue("credit_card_expiration_month"), 10, 32)
		ccYear, _     = strconv.ParseInt(r.FormValue("credit_card_expiration_year"), 10, 32)
		ccCVV, _      = strconv.ParseInt(r.FormValue("credit_card_cvv"), 10, 32)
	)

	payload := validator.PlaceOrderPayload{
		Email:         email,
		StreetAddress: streetAddress,
		ZipCode:       zipCode,
		City:          city,
		State:         state,
		Country:       country,
		CcNumber:      ccNumber,
		CcMonth:       ccMonth,
		CcYear:        ccYear,
		CcCVV:         ccCVV,
	}
	if err := payload.Validate(); err != nil {
		renderHTTPError(log, r, w, validator.ValidationErrorResponse(err), http.StatusUnprocessableEntity)
		return
	}

	orderID, err := checkoutOrderID(r, sessionID(r))
	if err != nil {
		renderHTTPError(log, r, w, err, http.StatusBadRequest)
		return
	}
	existing, existingErr := fe.storefrontQuery(r.Context(), "order", storefrontQueryRequest{OrderID: orderID, UserID: sessionID(r)})
	if existingErr == nil && existing.Order != nil {
		writeOrderResponse(w, r, http.StatusAccepted, existing.Order)
		return
	}
	if existingErr != nil && !errors.Is(existingErr, errProjectionNotFound) {
		w.Header().Set("Retry-After", "1")
		renderStorefrontError(log, r, w, errors.Wrap(existingErr, "could not verify idempotent order status"))
		return
	}
	view, err := fe.storefrontQuery(r.Context(), "cart", storefrontQueryRequest{UserID: sessionID(r), CurrencyCode: currentCurrency(r)})
	if err != nil {
		renderStorefrontError(log, r, w, errors.Wrap(err, "could not retrieve checkout snapshot"))
		return
	}
	if len(view.Items) == 0 {
		renderHTTPError(log, r, w, errors.New("cannot check out an empty cart"), http.StatusConflict)
		return
	}
	card := paymentCard{Number: payload.CcNumber, ExpirationMonth: int32(payload.CcMonth), ExpirationYear: int32(payload.CcYear), CVV: int32(payload.CcCVV)}
	token, err := fe.tokenizePayment(r.Context(), orderID, card)
	if err != nil {
		renderHTTPError(log, r, w, errors.Wrap(err, "payment tokenization failed"), http.StatusUnprocessableEntity)
		return
	}
	address := &commonv1.PostalAddress{StreetAddress: payload.StreetAddress, City: payload.City, State: payload.State,
		ZipCode: int32(payload.ZipCode), Country: payload.Country}
	if err := fe.publishOrder(r.Context(), orderID, sessionID(r), payload.Email, currentCurrency(r), address, token,
		view.CartVersion, view.CatalogRevision, view.RateRevision); err != nil {
		w.Header().Set("Retry-After", "1")
		renderHTTPError(log, r, w, errors.Wrap(err, "could not queue order"), http.StatusServiceUnavailable)
		return
	}
	log.WithField("order_id", orderID).Info("order queued")
	writeAcceptedOrder(w, r, orderID)
}

func (fe *frontendServer) assistantHandler(w http.ResponseWriter, r *http.Request) {
	log := r.Context().Value(ctxKeyLog{}).(logrus.FieldLogger)
	view, err := fe.storefrontQuery(r.Context(), "currencies", storefrontQueryRequest{CurrencyCode: currentCurrency(r)})
	if err != nil {
		renderStorefrontError(log, r, w, errors.Wrap(err, "could not retrieve currencies"))
		return
	}

	if err := templates.ExecuteTemplate(w, "assistant", injectCommonTemplateData(r, map[string]interface{}{
		"show_currency": false,
		"currencies":    filterCurrencies(view.Currencies),
	})); err != nil {
		log.Println(err)
	}
}

func (fe *frontendServer) logoutHandler(w http.ResponseWriter, r *http.Request) {
	log := r.Context().Value(ctxKeyLog{}).(logrus.FieldLogger)
	log.Debug("logging out")
	for _, c := range r.Cookies() {
		c.Expires = time.Now().Add(-time.Hour * 24 * 365)
		c.MaxAge = -1
		http.SetCookie(w, c)
	}
	w.Header().Set("Location", baseUrl+"/")
	w.WriteHeader(http.StatusFound)
}

func (fe *frontendServer) getProductByID(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["ids"]
	if id == "" {
		return
	}

	ids := strings.Split(id, ",")
	view, err := fe.storefrontQuery(r.Context(), "product-meta", storefrontQueryRequest{ProductIDs: ids})
	if err != nil {
		if errors.Is(err, errProjectionNotFound) {
			http.NotFound(w, r)
		} else {
			http.Error(w, "product projection unavailable", http.StatusServiceUnavailable)
		}
		return
	}
	var payload interface{} = view.ProductMeta
	if len(ids) == 1 && len(view.ProductMeta) == 1 {
		payload = view.ProductMeta[0]
	}
	jsonData, err := json.Marshal(payload)
	if err != nil {
		fmt.Println(err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(jsonData)
}

func (fe *frontendServer) chatBotHandler(w http.ResponseWriter, r *http.Request) {
	log := r.Context().Value(ctxKeyLog{}).(logrus.FieldLogger)
	if !assistantEnabled || fe.shoppingAssistantSvcAddr == "" {
		http.Error(w, "shopping assistant is not enabled", http.StatusNotFound)
		return
	}
	type Response struct {
		Message string `json:"message"`
	}

	type LLMResponse struct {
		Content string         `json:"content"`
		Details map[string]any `json:"details"`
	}

	var response LLMResponse

	url := "http://" + fe.shoppingAssistantSvcAddr
	req, err := http.NewRequest(http.MethodPost, url, r.Body)
	if err != nil {
		renderHTTPError(log, r, w, errors.Wrap(err, "failed to create request"), http.StatusInternalServerError)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		renderHTTPError(log, r, w, errors.Wrap(err, "failed to send request"), http.StatusInternalServerError)
		return
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		renderHTTPError(log, r, w, errors.Wrap(err, "failed to read response"), http.StatusInternalServerError)
		return
	}

	fmt.Printf("%+v\n", body)
	fmt.Printf("%+v\n", res)

	err = json.Unmarshal(body, &response)
	if err != nil {
		renderHTTPError(log, r, w, errors.Wrap(err, "failed to unmarshal body"), http.StatusInternalServerError)
		return
	}

	// respond with the same message
	json.NewEncoder(w).Encode(Response{Message: response.Content})

	w.WriteHeader(http.StatusOK)
}

func (fe *frontendServer) setCurrencyHandler(w http.ResponseWriter, r *http.Request) {
	log := r.Context().Value(ctxKeyLog{}).(logrus.FieldLogger)
	cur := r.FormValue("currency_code")
	payload := validator.SetCurrencyPayload{Currency: cur}
	if err := payload.Validate(); err != nil {
		renderHTTPError(log, r, w, validator.ValidationErrorResponse(err), http.StatusUnprocessableEntity)
		return
	}
	log.WithField("curr.new", payload.Currency).WithField("curr.old", currentCurrency(r)).
		Debug("setting currency")

	if payload.Currency != "" {
		http.SetCookie(w, &http.Cookie{
			Name:   cookieCurrency,
			Value:  payload.Currency,
			MaxAge: cookieMaxAge,
		})
	}
	referer := r.Header.Get("referer")
	if referer == "" {
		referer = baseUrl + "/"
	}
	w.Header().Set("Location", referer)
	w.WriteHeader(http.StatusFound)
}

func renderHTTPError(log logrus.FieldLogger, r *http.Request, w http.ResponseWriter, err error, code int) {
	log.WithField("error", err).Error("request error")
	errMsg := fmt.Sprintf("%+v", err)

	w.WriteHeader(code)

	if templateErr := templates.ExecuteTemplate(w, "error", injectCommonTemplateData(r, map[string]interface{}{
		"error":       errMsg,
		"status_code": code,
		"status":      http.StatusText(code),
	})); templateErr != nil {
		log.Println(templateErr)
	}
}

func renderStorefrontError(log logrus.FieldLogger, r *http.Request, w http.ResponseWriter, err error) {
	code := http.StatusServiceUnavailable
	if errors.Is(err, errProjectionNotFound) {
		code = http.StatusNotFound
	} else if errors.Is(err, errInvalidCurrency) {
		code = http.StatusUnprocessableEntity
	}
	renderHTTPError(log, r, w, err, code)
}

func injectCommonTemplateData(r *http.Request, payload map[string]interface{}) map[string]interface{} {
	data := map[string]interface{}{
		"session_id":        sessionID(r),
		"request_id":        r.Context().Value(ctxKeyRequestID{}),
		"user_currency":     currentCurrency(r),
		"platform_css":      plat.css,
		"platform_name":     plat.provider,
		"is_cymbal_brand":   isCymbalBrand,
		"assistant_enabled": assistantEnabled,
		"deploymentDetails": deploymentDetailsMap,
		"frontendMessage":   frontendMessage,
		"currentYear":       time.Now().Year(),
		"baseUrl":           baseUrl,
	}

	for k, v := range payload {
		data[k] = v
	}

	return data
}

func currentCurrency(r *http.Request) string {
	c, _ := r.Cookie(cookieCurrency)
	if c != nil {
		return c.Value
	}
	return defaultCurrency
}

func sessionID(r *http.Request) string {
	v := r.Context().Value(ctxKeySessionID{})
	if v != nil {
		return v.(string)
	}
	return ""
}

func renderMoney(money pb.Money) string {
	currencyLogo := renderCurrencyLogo(money.GetCurrencyCode())
	return fmt.Sprintf("%s%d.%02d", currencyLogo, money.GetUnits(), money.GetNanos()/10000000)
}

func renderCurrencyLogo(currencyCode string) string {
	logos := map[string]string{
		"USD": "$",
		"CAD": "$",
		"JPY": "¥",
		"EUR": "€",
		"TRY": "₺",
		"GBP": "£",
	}

	logo := "$" //default
	if val, ok := logos[currencyCode]; ok {
		logo = val
	}
	return logo
}

func stringinSlice(slice []string, val string) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}
