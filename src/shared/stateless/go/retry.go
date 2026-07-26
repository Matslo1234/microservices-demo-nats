// Copyright 2026 Google LLC
// Licensed under the Apache License, Version 2.0 (the "License");

package stateless

import (
	"context"
	"errors"
	"math/rand/v2"
	"net"
	"strings"
	"time"
)

type RetryClass uint8

const (
	RetryPermanent RetryClass = iota
	RetryConflict
	RetryDependency
)

// ClassifyRetry gives handlers one shared distinction between conflicts,
// transient dependency failures, and poison/permanent input errors.
func ClassifyRetry(err error) RetryClass {
	if err == nil {
		return RetryPermanent
	}
	if errors.Is(err, ErrConflict) || errors.Is(err, ErrLeaseHeld) ||
		errors.Is(err, ErrLeaseLost) {
		return RetryConflict
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return RetryDependency
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, ErrInvalidCommit) ||
		errors.Is(err, ErrLeaseComplete) {
		return RetryPermanent
	}
	var temporary interface{ Temporary() bool }
	if errors.As(err, &temporary) && temporary.Temporary() {
		return RetryDependency
	}
	var networkError net.Error
	if errors.As(err, &networkError) {
		return RetryDependency
	}
	message := strings.ToUpper(err.Error())
	for _, marker := range []string{
		"MOVED ", "ASK ", "TRYAGAIN", "CLUSTERDOWN", "LOADING ",
		"CONNECTION RESET", "CONNECTION REFUSED", "I/O TIMEOUT",
	} {
		if strings.Contains(message, marker) {
			return RetryDependency
		}
	}
	for _, marker := range []string{
		"WRONG LAST SEQUENCE", "WRONG LAST SEQ", "REVISION MISMATCH",
	} {
		if strings.Contains(message, marker) {
			return RetryConflict
		}
	}
	return RetryPermanent
}

// Backoff returns capped exponential delay with full jitter.
func Backoff(attempt int, minimum, maximum time.Duration) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	if minimum <= 0 {
		minimum = time.Millisecond
	}
	if maximum < minimum {
		maximum = minimum
	}
	capDelay := minimum
	for step := 0; step < attempt && capDelay < maximum; step++ {
		if capDelay > maximum/2 {
			capDelay = maximum
		} else {
			capDelay *= 2
		}
	}
	if capDelay > maximum {
		capDelay = maximum
	}
	if capDelay <= 1 {
		return capDelay
	}
	return time.Duration(rand.Int64N(int64(capDelay) + 1))
}
