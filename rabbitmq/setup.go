package rabbitmq

import (
	"log/slog"
)

// Direct Exchange untuk User Events
func SetupUserExchange(rmq *RabbitMQ) error {
	ch, err := rmq.GetChannel()
	if err != nil {
		return err
	}

	// Declare exchange
	err = ch.ExchangeDeclare(
		"user.events", // name
		"direct",      // type : Tipe routing (direct, topic, fanout, headers)
		true,          // durable : Exchange akan tetap ada meskipun rabbitmq restart)
		false,         // auto-deleted : Antrian akan dihapus jika tidak ada yang menggunakan
		false,         // internal : Exchange tidak dapat menerima pesan secara langsung
		false,         // no-wait : Exchange akan dibuat tanpa menunggu konfirmasi
		nil,           // arguments : Argumen tambahan
	)
	if err != nil {
		return err
	}

	slog.Info("Exchange 'user.events' created")
	return nil
}

// setup queue for welcome email
func SetupWelcomeEmailQueue(rmq *RabbitMQ) error {
	ch, err := rmq.GetChannel()
	if err != nil {
		return err
	}

	// Declare queue
	_, err = ch.QueueDeclare(
		"user.welcome.email", // name
		true,                 // durable
		false,                // delete when unused
		false,                // exclusive
		false,                // no-wait
		nil,                  // arguments
	)
	if err != nil {
		return err
	}

	// Bind queue to exchange
	err = ch.QueueBind(
		"user.welcome.email", // queue name -> [service-name].[purpose].[media]
		"user.registered",    // routing key -> [service-name].[action, like create, update, delete]
		"user.events",        // exchange -> [service-name].[event-type]
		false,
		nil,
	)
	if err != nil {
		return err
	}

	slog.Info("Queue 'user.welcome.email' created and bound")
	return nil
}

// SetupOrderExchange setup exchange untuk order events
func SetupOrderExchange(rmq *RabbitMQ) error {
	ch, err := rmq.GetChannel()
	if err != nil {
		return err
	}

	// Declare Topic Exchange
	err = ch.ExchangeDeclare(
		"order.events", // name
		"topic",        // type - TOPIC untuk routing patterns
		true,           // durable
		false,          // auto-deleted
		false,          // internal
		false,          // no-wait
		nil,            // arguments
	)
	if err != nil {
		return err
	}

	slog.Info("Exchange 'order.events' created (topic)")
	return nil
}

// queue setup for order email notifications
func SetupOrderEmailQueue(rmq *RabbitMQ) error {
	ch, err := rmq.GetChannel()
	if err != nil {
		return err
	}

	// Declare queue
	_, err = ch.QueueDeclare(
		"order.email.notifications", // name
		true,                        // durable
		false,                       // auto-delete
		false,                       // exclusive
		false,                       // no-wait
		nil,                         // arguments
	)
	if err != nil {
		return err
	}

	// Bind dengan wildcard - consume ALL order status changes
	err = ch.QueueBind(
		"order.email.notifications", // queue name
		"order.status.*",            // routing key pattern - * matches single word
		"order.events",              // exchange
		false,
		nil,
	)
	if err != nil {
		return err
	}

	slog.Info("Queue 'order.email.notifications' created and bound to order.status.*")
	return nil
}

// queue setup for order history update
func SetupOrderHistoryQueue(rmq *RabbitMQ) error {
	ch, err := rmq.GetChannel()
	if err != nil {
		return err
	}

	// Declare queue
	_, err = ch.QueueDeclare(
		"order.user.history", // name
		true,                 // durable
		false,                // auto-delete
		false,                // exclusive
		false,                // no-wait
		nil,                  // arguments
	)
	if err != nil {
		return err
	}

	// Bind multiple routing keys - only interested in paid & delivered
	routingKeys := []string{
		"order.status.paid",
		"order.status.delivered",
	}

	for _, key := range routingKeys {
		err = ch.QueueBind(
			"order.user.history", // queue name
			key,                  // routing key
			"order.events",       // exchange
			false,
			nil,
		)
		if err != nil {
			return err
		}
	}

	slog.Info("Queue 'order.user.history' created with selective bindings")
	return nil
}

func SetupProductExchange(rmq *RabbitMQ) error {
	ch, err := rmq.GetChannel()
	if err != nil {
		return err
	}

	return ch.ExchangeDeclare(
		"product.events", // name
		"topic",          // type
		true,             // durable
		false,            // auto-deleted
		false,            // internal
		false,            // no-wait
		nil,              // arguments
	)
}

// Setup exchange untuk blog events
func SetupBlogExchange(rmq *RabbitMQ) error {
	ch, err := rmq.GetChannel()
	if err != nil {
		return err
	}

	err = ch.ExchangeDeclare(
		"blog.events", // name
		"topic",       // type
		true,          // durable
		false,         // auto-deleted
		false,         // internal
		false,         // no-wait
		nil,           // arguments
	)
	if err != nil {
		return err
	}

	slog.Info("Exchange 'blog.events' created")
	return nil
}

// Setup queue dengan DLQ dan priority
func SetupBlogNotificationQueue(rmq *RabbitMQ) error {
	ch, err := rmq.GetChannel()
	if err != nil {
		return err
	}

	// Create Dead Letter Exchange
	err = ch.ExchangeDeclare(
		"blog.notifications.dlx", // Dead Letter Exchange
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	// Create Dead Letter Queue
	_, err = ch.QueueDeclare(
		"blog.notifications.failed", // DLQ name
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	// Bind DLQ to DLX
	err = ch.QueueBind(
		"blog.notifications.failed",
		"failed",
		"blog.notifications.dlx",
		false,
		nil,
	)
	if err != nil {
		return err
	}

	// Create Main Queue with DLQ configuration
	args := make(map[string]interface{})
	args["x-dead-letter-exchange"] = "blog.notifications.dlx"
	args["x-dead-letter-routing-key"] = "failed"
	args["x-message-ttl"] = int32(3600000) // 1 hour TTL
	args["x-max-priority"] = int32(10)     // Priority 1-10

	_, err = ch.QueueDeclare(
		"blog.follower.notifications", // Main queue
		true,
		false,
		false,
		false,
		args, // DLQ + TTL + Priority config
	)
	if err != nil {
		return err
	}

	// 5. Bind main queue to exchange
	err = ch.QueueBind(
		"blog.follower.notifications",
		"blog.published",
		"blog.events",
		false,
		nil,
	)
	if err != nil {
		return err
	}

	slog.Info("Blog notification queue created with DLQ, TTL, and Priority")
	return nil
}
