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
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	telemetry "github.com/GoogleCloudPlatform/microservices-demo/src/shared/telemetry/go"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

type ctxKeyLog struct{}
type ctxKeyRequestID struct{}

var (
	sessionCookieKeyOnce  sync.Once
	sessionCookieKeyValue [sha256.Size]byte
)

type logHandler struct {
	log  *logrus.Logger
	next http.Handler
}

type responseRecorder struct {
	b      int
	status int
	w      http.ResponseWriter
}

func (r *responseRecorder) Header() http.Header { return r.w.Header() }

// Unwrap lets http.ResponseController reach optional interfaces such as
// http.Flusher on the underlying writer. Streaming responses must continue to
// work when debug response accounting is enabled.
func (r *responseRecorder) Unwrap() http.ResponseWriter { return r.w }

func (r *responseRecorder) Write(p []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.w.Write(p)
	r.b += n
	return n, err
}

func (r *responseRecorder) WriteHeader(statusCode int) {
	r.status = statusCode
	r.w.WriteHeader(statusCode)
}

func (lh *logHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	requestID := uuid.NewString()
	ctx = context.WithValue(ctx, ctxKeyRequestID{}, requestID)
	telemetry.SetCorrelationID(ctx, requestID)

	log := lh.log.WithFields(logrus.Fields{
		"http.req.path":   r.URL.Path,
		"http.req.method": r.Method,
		"http.req.id":     requestID,
		"correlation_id":  requestID,
	})
	if v, ok := r.Context().Value(ctxKeySessionID{}).(string); ok {
		log = log.WithField("session", v)
	}

	nextWriter := w
	if lh.log.IsLevelEnabled(logrus.DebugLevel) {
		start := time.Now()
		rr := &responseRecorder{w: w}
		nextWriter = rr
		log.Debug("request started")
		defer func() {
			log.WithFields(logrus.Fields{
				"http.resp.took_ms": int64(time.Since(start) / time.Millisecond),
				"http.resp.status":  rr.status,
				"http.resp.bytes":   rr.b}).Debug("request complete")
		}()
	}

	ctx = context.WithValue(ctx, ctxKeyLog{}, log)
	r = r.WithContext(ctx)
	lh.next.ServeHTTP(nextWriter, r)
}

func ensureSessionID(next http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var sessionID string
		c, err := r.Cookie(cookieSessionID)
		if err == nil {
			sessionID, err = verifySessionCookie(c.Value)
		}
		if err != nil {
			if os.Getenv("ENABLE_SINGLE_SHARED_SESSION") == "true" {
				// Hard coded user id, shared across sessions
				sessionID = "12345678-1234-1234-1234-123456789123"
			} else {
				u, _ := uuid.NewRandom()
				sessionID = u.String()
			}
			http.SetCookie(w, &http.Cookie{
				Name:     cookieSessionID,
				Value:    signSessionCookie(sessionID),
				Path:     "/",
				MaxAge:   cookieMaxAge,
				HttpOnly: true,
				SameSite: http.SameSiteLaxMode,
				Secure:   r.TLS != nil,
			})
		}
		ctx := context.WithValue(r.Context(), ctxKeySessionID{}, sessionID)
		r = r.WithContext(ctx)
		next.ServeHTTP(w, r)
	}
}

func sessionCookieKey() []byte {
	sessionCookieKeyOnce.Do(func() {
		secret := os.Getenv("FRONTEND_COOKIE_KEY")
		if secret == "" {
			// The frontend NATS credential is already a replica-shared Kubernetes
			// Secret. Domain separation prevents using its raw value as a cookie key.
			secret = os.Getenv("NATS_PASSWORD")
		}
		sessionCookieKeyValue = sha256.Sum256(
			[]byte("online-boutique.frontend-cookie.v1\x00" + secret),
		)
	})
	return sessionCookieKeyValue[:]
}

func sessionCookieMAC(sessionID string) []byte {
	mac := hmac.New(sha256.New, sessionCookieKey())
	_, _ = mac.Write([]byte(sessionID))
	return mac.Sum(nil)
}

func signSessionCookie(sessionID string) string {
	return sessionID + "." + base64.RawURLEncoding.EncodeToString(sessionCookieMAC(sessionID))
}

func verifySessionCookie(value string) (string, error) {
	index := strings.LastIndexByte(value, '.')
	if index <= 0 || index == len(value)-1 {
		return "", http.ErrNoCookie
	}
	sessionID, encodedMAC := value[:index], value[index+1:]
	if _, err := uuid.Parse(sessionID); err != nil {
		return "", http.ErrNoCookie
	}
	actual, err := base64.RawURLEncoding.DecodeString(encodedMAC)
	if err != nil {
		return "", http.ErrNoCookie
	}
	if !hmac.Equal(actual, sessionCookieMAC(sessionID)) {
		return "", http.ErrNoCookie
	}
	return sessionID, nil
}
