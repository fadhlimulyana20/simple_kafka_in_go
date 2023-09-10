package producer

import (
	"fmt"
	"testing"

	"github.com/IBM/sarama/mocks"
)

func TestSendMessage(t *testing.T) {
	t.Run("Send Message OK", func(t *testing.T) {
		// Create Producer Mock
		mP := mocks.NewSyncProducer(t, nil)
		// Create expect producer for success send message
		mP.ExpectSendMessageAndSucceed()
		kafka := &KafkaProducer{
			Producer: mP,
		}

		msg := "Message 1"

		if err := kafka.SendMessage("test_topic", msg); err != nil {
			t.Errorf("send message should not be error but have: %v", err)
		}
	})

	t.Run("Send Message Not OK", func(t *testing.T) {
		// Create Producer Mock
		mP := mocks.NewSyncProducer(t, nil)
		mP.ExpectSendMessageAndFail(fmt.Errorf("Error"))
		kafka := &KafkaProducer{
			Producer: mP,
		}

		msg := "Message 2"

		if err := kafka.SendMessage("test_topic", msg); err == nil {
			t.Error("this should be error")
		}
	})
}
