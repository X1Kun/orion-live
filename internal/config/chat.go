package config

import (
	"errors"
	"time"
)

type Chat struct {
	PublishTimeout    time.Duration
	AdmissionTimeout  time.Duration
	UserRatePerSecond int
	UserBurst         int
	RoomRatePerSecond int
	RoomBurst         int
}

func loadChat() (Chat, error) {
	publishTimeout, err := envDuration("CHAT_PUBLISH_TIMEOUT", 5*time.Second)
	if err != nil {
		return Chat{}, err
	}
	admissionTimeout, err := envDuration("CHAT_ADMISSION_TIMEOUT", 100*time.Millisecond)
	if err != nil {
		return Chat{}, err
	}
	userRate, err := envInt("CHAT_USER_RATE_PER_SECOND", 5)
	if err != nil {
		return Chat{}, err
	}
	userBurst, err := envInt("CHAT_USER_BURST", 10)
	if err != nil {
		return Chat{}, err
	}
	roomRate, err := envInt("CHAT_ROOM_RATE_PER_SECOND", 100)
	if err != nil {
		return Chat{}, err
	}
	roomBurst, err := envInt("CHAT_ROOM_BURST", 200)
	if err != nil {
		return Chat{}, err
	}
	return Chat{
		PublishTimeout:    publishTimeout,
		AdmissionTimeout:  admissionTimeout,
		UserRatePerSecond: userRate,
		UserBurst:         userBurst,
		RoomRatePerSecond: roomRate,
		RoomBurst:         roomBurst,
	}, nil
}

func (c Chat) validate() error {
	if c.PublishTimeout <= 0 {
		return errors.New("CHAT_PUBLISH_TIMEOUT must be positive")
	}
	if c.AdmissionTimeout <= 0 {
		return errors.New("CHAT_ADMISSION_TIMEOUT must be positive")
	}
	if c.UserRatePerSecond <= 0 || c.UserBurst <= 0 {
		return errors.New("CHAT_USER_RATE_PER_SECOND and CHAT_USER_BURST must be positive")
	}
	if c.RoomRatePerSecond <= 0 || c.RoomBurst <= 0 {
		return errors.New("CHAT_ROOM_RATE_PER_SECOND and CHAT_ROOM_BURST must be positive")
	}
	return nil
}
