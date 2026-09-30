package rabbitmq

import (
	"context"
	"fmt"

	"github.com/X1Kun/orion-live/internal/config"
	"github.com/X1Kun/orion-live/internal/messaging"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	InteractionExchangeName           = "orion.interaction.events"
	PersistenceQueueName              = "orion.interaction.persistence"
	PersistenceDeadLetterExchangeName = "orion.interaction.persistence.dlx"
	PersistenceDeadLetterQueueName    = "orion.interaction.persistence.dlq"
)

func InitializeCoreTopology(ctx context.Context, client *Client, cfg config.Persistence) error {
	channel, err := client.Channel(ctx)
	if err != nil {
		return err
	}
	defer channel.Close()
	return DeclareCoreTopology(channel, cfg)
}

func DeclareCoreTopology(channel *amqp.Channel, cfg config.Persistence) error {
	if err := declareInteractionExchange(channel); err != nil {
		return err
	}
	if err := declarePersistenceDeadLetterPath(channel); err != nil {
		return err
	}
	return declarePersistenceConsumerPath(channel, cfg)
}

func declareInteractionExchange(channel *amqp.Channel) error {
	if err := channel.ExchangeDeclare(InteractionExchangeName, "topic", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare interaction exchange: %w", err)
	}
	return nil
}

func declarePersistenceDeadLetterPath(channel *amqp.Channel) error {
	if err := channel.ExchangeDeclare(PersistenceDeadLetterExchangeName, "topic", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare persistence DLX: %w", err)
	}
	if _, err := channel.QueueDeclare(
		PersistenceDeadLetterQueueName,
		true,
		false,
		false,
		false,
		amqp.Table{"x-queue-type": "quorum"},
	); err != nil {
		return fmt.Errorf("declare persistence DLQ: %w", err)
	}
	if err := channel.QueueBind(
		PersistenceDeadLetterQueueName,
		"#",
		PersistenceDeadLetterExchangeName,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("bind persistence DLQ: %w", err)
	}
	return nil
}

func declarePersistenceConsumerPath(channel *amqp.Channel, cfg config.Persistence) error {
	persistenceArguments := amqp.Table{
		"x-queue-type":           "quorum",
		"x-delayed-retry-type":   "failed",
		"x-delayed-retry-min":    cfg.RetryMinDelay.Milliseconds(),
		"x-delayed-retry-max":    cfg.RetryMaxDelay.Milliseconds(),
		"x-delivery-limit":       cfg.DeliveryLimit,
		"x-dead-letter-exchange": PersistenceDeadLetterExchangeName,
		"x-dead-letter-strategy": "at-least-once",
		"x-overflow":             "reject-publish",
	}
	if _, err := channel.QueueDeclare(
		PersistenceQueueName,
		true,
		false,
		false,
		false,
		persistenceArguments,
	); err != nil {
		return fmt.Errorf("declare persistence queue: %w", err)
	}
	if err := channel.QueueBind(
		PersistenceQueueName,
		string(messaging.EventTypeChatMessageAccepted),
		InteractionExchangeName,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("bind persistence queue: %w", err)
	}
	return nil
}
