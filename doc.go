// Package kolostore persists kolo-log logs in an ordered key-value backend.
// Many logs share one store; each write is one atomic, durable batch.
//
// This package is experimental and its API may change before v1.0.0.
package kolostore
