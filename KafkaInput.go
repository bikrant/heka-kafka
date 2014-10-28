package kafka

import (
	"fmt"
        "errors"
        "strings"
        "time"
        "encoding/json"
        "github.com/Shopify/sarama"
        "github.com/mozilla-services/heka/message"
        . "github.com/mozilla-services/heka/pipeline"
)

type KafkaInputConfig struct {
        Address string
        Id string
        Cluster string
        Topic string
        CompressionCodec sarama.CompressionCodec
        MaxBufferTime time.Duration
        MaxBufferedKB uint32
}

type KafkaInput struct {
        config *KafkaInputConfig
        addrs []string
        client *sarama.Client
        producer *sarama.Producer
}

func (ao *KafkaInput) ConfigStruct() interface{} {
        return &KafkaInputConfig{}
}

func (ao *KafkaInput) Init(config interface{}) (err error) {
        ao.config = config.(*KafkaInputConfig)
        ao.addrs = strings.Split(ao.config.Address, ",")
        if len(ao.addrs) == 1 && len(ao.addrs) == 0 {
                err = errors.New("invalid address")
        }

        err = ao.init()
        return
}

func (ao *KafkaInput) Run(or OutputRunner, h PluginHelper) (err error) {
        inChan := or.InChan()
        errChan := ao.producer.Errors()

        var pack *PipelinePack
        var msg *message.Message
        var topic string
        var key string

        ok := true
        for ok {
                select {
                case pack, ok = <-inChan:
                        if !ok {
                                break
                        }

                        msg = pack.Message
                        pack.Recycle()
                        
                        topic = msg.GetType()
                        key = ao.config.Id
                        message.NewStringField(msg, "cluster", ao.config.Cluster)
                        
                        b, err := json.Marshal(msg)
                        if err != nil {
                        	fmt.Println("error:", err)
                        	or.LogError(err)
                        	break
                        }

                        err = ao.producer.QueueMessage(topic,  sarama.StringEncoder(key), sarama.ByteEncoder(b))
                        if err != nil {
                        	fmt.Println("error:", err)
                                or.LogError(err)
                        }
                        break

                case err = <-errChan:
                        break
                }
        }
        return
}

func (ao *KafkaInput) CleanupForRestart() {
        ao.client.Close()
        ao.producer.Close()
        ao.init()
}

func (ao *KafkaInput) init() (err error) {
        cconf := sarama.NewClientConfig()
        ao.client, err = sarama.NewClient(ao.config.Id, ao.addrs, cconf)
        if err != nil {
                return
        }
        kconf := sarama.NewProducerConfig()
        kconf.Partitioner = sarama.NewHashPartitioner()

        kconf.Compression = ao.config.CompressionCodec
        kconf.MaxBufferTime = ao.config.MaxBufferTime * time.Millisecond
        kconf.MaxBufferedBytes = ao.config.MaxBufferedKB * 1024

        ao.producer, err = sarama.NewProducer(ao.client, kconf)
        if err != nil {
                return
        }
        return
}

func init() {
	RegisterPlugin("KafkaInput", func() interface{} {
		return new(KafkaInput)
	})
}
