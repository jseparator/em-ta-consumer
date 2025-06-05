package main

import (
	"context"
	"errors"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"strings"
	"sync"
	"time"
)

var km *KafkaManager

func init() {
	km = &KafkaManager{}
	km.ctx, km.cancel = context.WithCancel(context.Background())

	km.consumer = kafka.NewReader(kafka.ReaderConfig{
		Brokers:        strings.Split(cfg.Kafka.Brokers, ","),
		GroupID:        cfg.Kafka.GroupId,
		Topic:          cfg.Kafka.Topic,
		CommitInterval: time.Second,
		SessionTimeout: time.Minute,
		StartOffset:    kafka.FirstOffset,
		Dialer: &kafka.Dialer{
			ClientID:  cfg.Kafka.GroupId,
			Timeout:   time.Second * 5,
			KeepAlive: time.Hour * 72,
		},
	})
	log.Infof("KafkaConsumer Init: %s", cfg.Kafka.Brokers)
	go km.consume()
}

type KafkaManager struct {
	consumer *kafka.Reader
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

func (k *KafkaManager) Close() {
	k.cancel()
	k.wg.Wait()
}

func (k *KafkaManager) consume() {
	k.wg.Add(1)
	defer k.wg.Done()
	for {
		msg, err := k.consumer.ReadMessage(k.ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				break
			}
			log.Errorf("consume error: %v", err)
			continue
		}
		httpHandler.Handle(&msg)
	}
	_ = k.consumer.Close()
	log.Infof("KafkaConsumer Close")
}
