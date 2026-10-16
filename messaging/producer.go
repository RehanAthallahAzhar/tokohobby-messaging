package messaging

import (
	"context"
	"fmt"

	"github.com/RehanAthallahAzhar/tokohobby-messaging/kafka"
	"github.com/RehanAthallahAzhar/tokohobby-messaging/rabbitmq"
)

// producerAdapter wraps kafka.Producer to implement messaging.Producer
type kafkaProducerAdapter struct {
	*kafka.Producer
}

func (a *kafkaProducerAdapter) Publish(ctx context.Context, msg *Message) error {
	return a.Producer.Publish(ctx, &kafka.Message{
		Topic:     msg.Topic,
		Key:       msg.Key,
		Value:     msg.Value,
		Headers:   msg.Headers,
		Metadata:  msg.Metadata,
		Timestamp: msg.Timestamp,
	})
}

func (a *kafkaProducerAdapter) PublishBatch(ctx context.Context, messages []*Message) error {
	kafkaMsgs := make([]*kafka.Message, len(messages))
	for i, msg := range messages {
		kafkaMsgs[i] = &kafka.Message{
			Topic:     msg.Topic,
			Key:       msg.Key,
			Value:     msg.Value,
			Headers:   msg.Headers,
			Metadata:  msg.Metadata,
			Timestamp: msg.Timestamp,
		}
	}
	return a.Producer.PublishBatch(ctx, kafkaMsgs)
}

// rabbitmqProducerAdapter wraps rabbitmq.Producer to implement messaging.Producer
type rabbitmqProducerAdapter struct {
	*rabbitmq.Producer
}

func (a *rabbitmqProducerAdapter) Publish(ctx context.Context, msg *Message) error {
	return a.Producer.Publish(ctx, &rabbitmq.Message{
		Topic:     msg.Topic,
		Key:       msg.Key,
		Value:     msg.Value,
		Headers:   msg.Headers,
		Timestamp: msg.Timestamp,
	})
}

func (a *rabbitmqProducerAdapter) PublishBatch(ctx context.Context, messages []*Message) error {
	rmqMsgs := make([]*rabbitmq.Message, len(messages))
	for i, msg := range messages {
		rmqMsgs[i] = &rabbitmq.Message{
			Topic:     msg.Topic,
			Key:       msg.Key,
			Value:     msg.Value,
			Headers:   msg.Headers,
			Timestamp: msg.Timestamp,
		}
	}
	return a.Producer.PublishBatch(ctx, rmqMsgs)
}

// NewProducer creates a new producer based on broker type
func NewProducer(cfg *ProducerConfig) (Producer, error) {
	switch cfg.BrokerType {
	case "kafka":
		kafkaProd := kafka.NewProducerFromMessaging(
			cfg.Brokers,
			cfg.Topic,
			cfg.AsyncMode,
			cfg.BatchSize,
		)
		return &kafkaProducerAdapter{kafkaProd}, nil

	case "rabbitmq":
		rmqProd, err := rabbitmq.NewProducerFromMessaging(cfg.Brokers, cfg.Topic)
		if err != nil {
			return nil, fmt.Errorf("rabbitmq producer: %w", err)
		}
		return &rabbitmqProducerAdapter{rmqProd}, nil

	default:
		return nil, fmt.Errorf("unsupported broker type: %s", cfg.BrokerType)
	}
}

// NewConsumer creates a new consumer based on broker type
// TODO: Implement when consumer implementations are ready
/*
func NewConsumer(cfg *ConsumerConfig) (Consumer, error) {
	switch cfg.BrokerType {
	case "kafka":
		return kafka.NewConsumerFromMessaging(cfg.Brokers, cfg.Topic, cfg.GroupID)
	case "rabbitmq":
		return rabbitmq.NewConsumerFromMessaging(cfg.Brokers, cfg.Topic, cfg.PrefetchCount)
	default:
		return nil, fmt.Errorf("unsupported broker type: %s", cfg.BrokerType)
	}
}
*/
