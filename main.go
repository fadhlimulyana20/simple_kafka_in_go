package main

import (
	"kafka_go/producer"
	"time"

	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
)

func main() {
	kafkaCfg := getKafkaCfg("", "")
	producers, err := sarama.NewSyncProducer([]string{"localhost:9092"}, kafkaCfg)
	if err != nil {
		logrus.Errorf("unable to create kafka producer, got error: %v", err)
		return
	}

	defer func() {
		if err := producers.Close(); err != nil {
			logrus.Errorf("unable to stop kafka producer, got error: %v", err)
		}
		return
	}()

	logrus.Info("Success create kafka sync producer")

	kafka := &producer.KafkaProducer{
		Producer: producers,
	}

	kafka.SendMessage("test_topic", "Halo kafka")
}

func getKafkaCfg(username, password string) *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Net.WriteTimeout = 5 * time.Second
	cfg.Producer.Retry.Max = 0

	if username != "" {
		cfg.Net.SASL.Enable = true
		cfg.Net.SASL.User = username
		cfg.Net.SASL.Password = password
	}

	return cfg
}
