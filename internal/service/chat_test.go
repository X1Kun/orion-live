package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/X1Kun/orion-live/internal/config"
	"github.com/X1Kun/orion-live/internal/messaging"
)

const chatTestMessageID = "018f47a2-8e31-4f10-8af0-2bdac5812501"

func TestChatAcceptPublishesCanonicalEvent(t *testing.T) {
	publisher := &chatPublisherStub{}
	admission := &chatAdmissionStub{allowed: true}
	service := NewChatService(admission, publisher, chatTestConfig())
	accepted, err := service.Accept(context.Background(), 7, 42, "018F47A2-8E31-4F10-8AF0-2BDAC5812501", " hello ")
	if err != nil {
		t.Fatalf("Accept() error = %v", err)
	}
	if accepted.MessageID != chatTestMessageID || accepted.AcceptedAt.IsZero() {
		t.Fatalf("unexpected acceptance: %#v", accepted)
	}
	if publisher.event.EventType != messaging.EventTypeChatMessageAccepted || publisher.event.LiveSessionID != 7 || publisher.event.UserID != 42 {
		t.Fatalf("unexpected event envelope: %#v", publisher.event)
	}
	var payload messaging.ChatMessageAcceptedPayload
	if err := json.Unmarshal(publisher.event.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if payload.MessageID != chatTestMessageID || payload.Content != " hello " || payload.AcceptedAt != accepted.AcceptedAt {
		t.Fatalf("unexpected payload: %#v", payload)
	}
}

func TestChatAcceptRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name      string
		messageID string
		content   string
		want      error
	}{
		{name: "non UUID", messageID: "message-1", content: "hello", want: ErrInvalidChatMessageID},
		{name: "wrong UUID version", messageID: "018f47a2-8e31-3f10-8af0-2bdac5812501", content: "hello", want: ErrInvalidChatMessageID},
		{name: "empty content", messageID: chatTestMessageID, content: " \t", want: ErrInvalidChatContent},
		{name: "content too long", messageID: chatTestMessageID, content: string(make([]rune, maxChatContentRunes+1)), want: ErrInvalidChatContent},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			publisher := &chatPublisherStub{}
			admission := &chatAdmissionStub{allowed: true}
			service := NewChatService(admission, publisher, chatTestConfig())
			_, err := service.Accept(context.Background(), 7, 42, tt.messageID, tt.content)
			if !errors.Is(err, tt.want) {
				t.Fatalf("Accept() error = %v, want %v", err, tt.want)
			}
			if publisher.calls != 0 {
				t.Fatalf("Publish() calls = %d, want 0", publisher.calls)
			}
			if admission.calls != 0 {
				t.Fatalf("Allow() calls = %d, want 0", admission.calls)
			}
		})
	}
}

func TestChatAcceptBoundsPublish(t *testing.T) {
	publisher := &chatPublisherStub{waitForContext: true}
	cfg := chatTestConfig()
	cfg.PublishTimeout = 10 * time.Millisecond
	service := NewChatService(&chatAdmissionStub{allowed: true}, publisher, cfg)
	_, err := service.Accept(context.Background(), 7, 42, chatTestMessageID, "hello")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Accept() error = %v, want deadline exceeded", err)
	}
}

func TestChatAcceptEnforcesAdmission(t *testing.T) {
	tests := []struct {
		name      string
		admission *chatAdmissionStub
		want      error
	}{
		{name: "rate limited", admission: &chatAdmissionStub{}, want: ErrChatRateLimited},
		{name: "unavailable", admission: &chatAdmissionStub{err: errors.New("redis unavailable")}, want: ErrChatAdmissionUnavailable},
		{name: "timeout", admission: &chatAdmissionStub{waitForContext: true}, want: ErrChatAdmissionUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			publisher := &chatPublisherStub{}
			cfg := chatTestConfig()
			cfg.AdmissionTimeout = 10 * time.Millisecond
			service := NewChatService(tt.admission, publisher, cfg)
			_, err := service.Accept(context.Background(), 7, 42, chatTestMessageID, "hello")
			if !errors.Is(err, tt.want) {
				t.Fatalf("Accept() error = %v, want %v", err, tt.want)
			}
			if publisher.calls != 0 {
				t.Fatalf("Publish() calls = %d, want 0", publisher.calls)
			}
		})
	}
}

func chatTestConfig() config.Chat {
	return config.Chat{
		PublishTimeout: time.Second, AdmissionTimeout: time.Second,
		UserRatePerSecond: 5, UserBurst: 10, RoomRatePerSecond: 100, RoomBurst: 200,
	}
}

type chatAdmissionStub struct {
	allowed        bool
	err            error
	calls          int
	waitForContext bool
}

func (a *chatAdmissionStub) Allow(ctx context.Context, _, _ uint64) (bool, error) {
	a.calls++
	if a.waitForContext {
		<-ctx.Done()
		return false, ctx.Err()
	}
	return a.allowed, a.err
}

type chatPublisherStub struct {
	event          messaging.Event
	calls          int
	waitForContext bool
}

func (p *chatPublisherStub) Publish(ctx context.Context, event messaging.Event) error {
	p.calls++
	p.event = event
	if p.waitForContext {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}
