package messaging

import (
	"crypto/rand"
	"encoding/hex"
)

func NewCorrelationID() (string, error) {
	return newRandomID()
}

func NewEventID() (string, error) {
	return newRandomID()
}

func NewClaimToken() (string, error) {
	return newRandomID()
}

func newRandomID() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}
