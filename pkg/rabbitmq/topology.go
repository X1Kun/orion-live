package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/X1Kun/orion-live/internal/messaging"
	amqp "github.com/rabbitmq/amqp091-go"
)

const DefaultPersistenceRetryDelay = 5 * time.Second

var ErrInvalidRetryDelay = errors.New("persistence retry delay must be positive")

func InitializeCoreTopology(ctx context.Context, client *Client, retryDelay time.Duration) error {
	channel, err := client.Channel(ctx)
	if err != nil {
		return err
	}
	defer channel.Close()
	return DeclareCoreTopology(channel, retryDelay)
}

func DeclareCoreTopology(channel *amqp.Channel, retryDelay time.Duration) error {
	if retryDelay <= 0 {
		return ErrInvalidRetryDelay
	}
	if err := declareInteractionExchange(channel); err != nil {
		return err
	}
	if err := declarePersistenceDeadLetterPath(channel); err != nil {
		return err
	}
	if err := declarePersistenceRetryPath(channel, retryDelay); err != nil {
		return err
	}
	return declarePersistenceConsumerPath(channel)
}

func declareInteractionExchange(channel *amqp.Channel) error {
	if err := channel.ExchangeDeclare(messaging.ExchangeName, "topic", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare interaction exchange: %w", err)
	}
	return nil
}

func declarePersistenceDeadLetterPath(channel *amqp.Channel) error {
	if err := channel.ExchangeDeclare(messaging.PersistenceDLXName, "topic", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare persistence DLX: %w", err)
	}
	if _, err := channel.QueueDeclare(
		messaging.PersistenceDLQName,
		true,
		false,
		false,
		false,
		amqp.Table{"x-queue-type": "classic"},
	); err != nil {
		return fmt.Errorf("declare persistence DLQ: %w", err)
	}
	if err := channel.QueueBind(
		messaging.PersistenceDLQName,
		"#",
		messaging.PersistenceDLXName,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("bind persistence DLQ: %w", err)
	}
	return nil
}

func declarePersistenceRetryPath(channel *amqp.Channel, retryDelay time.Duration) error {
	if err := channel.ExchangeDeclare(messaging.PersistenceRetryExchangeName, "topic", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare persistence retry exchange: %w", err)
	}
	retryArguments := amqp.Table{
		"x-queue-type":           "classic",
		"x-message-ttl":          retryDelay.Milliseconds(),
		"x-dead-letter-exchange": messaging.ExchangeName,
	}
	if _, err := channel.QueueDeclare(
		messaging.PersistenceRetryQueueName,
		true,
		false,
		false,
		false,
		retryArguments,
	); err != nil {
		return fmt.Errorf("declare persistence retry queue: %w", err)
	}
	if err := channel.QueueBind(
		messaging.PersistenceRetryQueueName,
		"#",
		messaging.PersistenceRetryExchangeName,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("bind persistence retry queue: %w", err)
	}
	return nil
}

func declarePersistenceConsumerPath(channel *amqp.Channel) error {
	persistenceArguments := amqp.Table{
		"x-queue-type":           "classic",
		"x-dead-letter-exchange": messaging.PersistenceDLXName,
	}
	if _, err := channel.QueueDeclare(
		messaging.PersistenceQueueName,
		true,
		false,
		false,
		false,
		persistenceArguments,
	); err != nil {
		return fmt.Errorf("declare persistence queue: %w", err)
	}
	if err := channel.QueueBind(
		messaging.PersistenceQueueName,
		string(messaging.EventTypeChatMessageAccepted),
		messaging.ExchangeName,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("bind persistence queue: %w", err)
	}
	return nil
}
