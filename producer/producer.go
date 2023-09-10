package producer

import (
	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
)

type KafkaProducer struct {
	Producer sarama.SyncProducer
}

func (k *KafkaProducer) SendMessage(topic, msg string) error {
	kafkaMsg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(msg),
	}

	partition, offset, err := k.Producer.SendMessage(kafkaMsg)
	if err != nil {
		logrus.Errorf("send message error: %v", err)
		return err
	}

	logrus.Infof("send message success, topic: %v, partition: %v, offset: %v", topic, partition, offset)
	return nil
}
