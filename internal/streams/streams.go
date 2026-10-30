package streams

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/HowkaCoder/khan/internal/events"
)

const (
	CommandRetention   = 7 * 24 * time.Hour
	DLQRetention       = 30 * 24 * time.Hour
	DeduplicationLimit = 2 * time.Minute
	ReasonHeader       = "Khan-Reason"
)

func Connect(url, clientName string) (*nats.Conn, jetstream.JetStream, error) {
	conn, err := nats.Connect(url,
		nats.Name(clientName),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(500*time.Millisecond),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("не удалось подключиться к NATS: %w", err)
	}
	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("не удалось открыть JetStream: %w", err)
	}
	return conn, js, nil
}

func Ensure(ctx context.Context, js jetstream.JetStream) error {
	configs := []jetstream.StreamConfig{
		{
			Name:       events.StreamOrders,
			Subjects:   []string{"orders.>"},
			Storage:    jetstream.FileStorage,
			Retention:  jetstream.LimitsPolicy,
			MaxAge:     CommandRetention,
			Duplicates: DeduplicationLimit,
			Replicas:   1,
		},
		{
			Name:       events.StreamTrades,
			Subjects:   []string{"trades.>"},
			Storage:    jetstream.FileStorage,
			Retention:  jetstream.LimitsPolicy,
			MaxAge:     CommandRetention,
			Duplicates: DeduplicationLimit,
			Replicas:   1,
		},
		{
			Name:      events.StreamDLQ,
			Subjects:  []string{"dlq.>"},
			Storage:   jetstream.FileStorage,
			Retention: jetstream.LimitsPolicy,
			MaxAge:    DLQRetention,
		},
	}
	for _, config := range configs {
		if _, err := js.CreateOrUpdateStream(ctx, config); err != nil {
			return fmt.Errorf("не удалось создать стрим %s: %w", config.Name, err)
		}
	}
	return nil
}

func PublishDLQ(ctx context.Context, js jetstream.JetStream, subject string, data []byte, reason string) error {
	message := nats.NewMsg(subject)
	message.Data = data
	message.Header.Set(ReasonHeader, reason)
	if _, err := js.PublishMsg(ctx, message); err != nil {
		return fmt.Errorf("не удалось отправить сообщение в DLQ: %w", err)
	}
	return nil
}
