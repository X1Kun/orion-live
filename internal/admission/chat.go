package admission

import (
	"context"
	"fmt"

	"github.com/X1Kun/orion-live/internal/config"
	redisclient "github.com/go-redis/redis/v8"
)

var chatTokenBucketScript = redisclient.NewScript(`
local now_parts = redis.call('TIME')
local now_ms = tonumber(now_parts[1]) * 1000 + math.floor(tonumber(now_parts[2]) / 1000)

local function refill(key, capacity, rate)
    local state = redis.call('HMGET', key, 'tokens', 'updated_at_ms')
    local tokens = tonumber(state[1])
    local updated_at_ms = tonumber(state[2])
    if tokens == nil or updated_at_ms == nil then
        return capacity
    end
    local elapsed_ms = math.max(0, now_ms - updated_at_ms)
    return math.min(capacity, tokens + elapsed_ms * rate / 1000)
end

local user_capacity = tonumber(ARGV[1])
local user_rate = tonumber(ARGV[2])
local room_capacity = tonumber(ARGV[3])
local room_rate = tonumber(ARGV[4])
local user_tokens = refill(KEYS[1], user_capacity, user_rate)
local room_tokens = refill(KEYS[2], room_capacity, room_rate)
local allowed = 0

if user_tokens >= 1 and room_tokens >= 1 then
    user_tokens = user_tokens - 1
    room_tokens = room_tokens - 1
    allowed = 1
end

redis.call('HSET', KEYS[1], 'tokens', user_tokens, 'updated_at_ms', now_ms)
redis.call('HSET', KEYS[2], 'tokens', room_tokens, 'updated_at_ms', now_ms)
redis.call('PEXPIRE', KEYS[1], math.max(1000, math.ceil(user_capacity / user_rate * 2000)))
redis.call('PEXPIRE', KEYS[2], math.max(1000, math.ceil(room_capacity / room_rate * 2000)))
return allowed
`)

type ChatLimiter struct {
	redis  redisclient.Scripter
	config config.Chat
}

func NewChatLimiter(redis redisclient.Scripter, cfg config.Chat) *ChatLimiter {
	return &ChatLimiter{redis: redis, config: cfg}
}

func (l *ChatLimiter) Allow(ctx context.Context, liveSessionID, userID uint64) (bool, error) {
	keyPrefix := fmt.Sprintf("orion:chat:{%d}:admission", liveSessionID)
	result, err := chatTokenBucketScript.Run(
		ctx,
		l.redis,
		[]string{fmt.Sprintf("%s:user:%d", keyPrefix, userID), keyPrefix + ":room"},
		l.config.UserBurst,
		l.config.UserRatePerSecond,
		l.config.RoomBurst,
		l.config.RoomRatePerSecond,
	).Int64()
	if err != nil {
		return false, fmt.Errorf("apply Chat admission: %w", err)
	}
	return result == 1, nil
}
