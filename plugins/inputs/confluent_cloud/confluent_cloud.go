package confluent_cloud

import (
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/parsers"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

const (
	defaultSessionTimeout = 6000
	defaultAutoOffsetReset = "earliest"
)

var ConfluentKafkaConfig = `
	## Broker(s) of Confluent Kafka cluster. If you have multiple brokers, provide a comma separated list	
	brokers = "localhost:9092"

	## Kafka topics to consume from
	topics = ["test"]

	## Consumer group
	consumer_group = "my-group"

	## Security protocol
	security_protocol = "SASL_SSL"

	## SASL mechanism
	sasl_mechanisms = "PLAIN",

	## Confluent Cloud authentication
	confluent_api_key = "my-api-key"
	confluent_api_secret = "my-api-secret"

	## Session timeout (ms)
	session_timeout = 6000

	## Auto Offset Reset. Can be set to earliest or latest
	auto_offset_reset	= "earliest"

	## Data format to consume.
  	## Each data format has its own unique set of configuration options, read
  	## more about them here:
  	## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md
  	data_format = "json"
`

type ConfluentKafka struct {
	Brokers 				string		`toml:"brokers"`
	Topics					[]string 	`toml:"topics"`
	ConsumerGroup 			string 		`toml:"consumer_group"`
	SecurityProtocol 		string 		`toml:"security_protocol"`
	SaslMechanisms 			string 		`toml:"sasl_mechanisms"`
	ConfluentApiKey 		string 		`toml:"confluent_api_key"`
	ConfluentApiSecret 		string 		`toml:"confluent_api_secret"`
	SessionTimeout			int 		`toml:"session_timeout"`
	AutoOffsetReset			string 		`toml:"auto_offset_reset"`

	metricsSetBuffer		[]*[]telegraf.Metric

	consumer				*kafka.Consumer
	acc           			telegraf.Accumulator
	consuming				bool

	// The parser will automatically be set by Telegraf core code because
	// this plugin implements the ParserInput interface (i.e. the SetParser method)
	parser 					parsers.Parser
	Log 					telegraf.Logger
}


func (c *ConfluentKafka) SampleConfig() string {
	return ConfluentKafkaConfig
}

func (c *ConfluentKafka) Description() string {
	return "Consume data from Confluent Cloud Kafka."
}

func (c *ConfluentKafka) SetParser(parser parsers.Parser) {
	c.parser = parser
}

func(c *ConfluentKafka) Init() error {

	if c.SessionTimeout == 0 {
		c.SessionTimeout = defaultSessionTimeout
	}

	if c.AutoOffsetReset == "" {
		c.AutoOffsetReset = defaultAutoOffsetReset
	}

	var err error
	c.consumer, err = kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":       c.Brokers,
		"group.id":                c.ConsumerGroup,
		"security.protocol":       c.SecurityProtocol,
		"sasl.mechanisms":         c.SaslMechanisms,
		"sasl.username":           c.ConfluentApiKey,
		"sasl.password":           c.ConfluentApiSecret,
		"session.timeout.ms":      c.SessionTimeout,
		"auto.offset.reset":       c.AutoOffsetReset,
	})
	if err != nil {
		return err
	}

	c.consumer.SubscribeTopics(c.Topics, nil)

	return nil
}


func (c *ConfluentKafka) Gather(acc telegraf.Accumulator) error {
	// Start at first run
	if !c.consuming {
		go c.Consume()
	}

	for _, metricSet := range c.metricsSetBuffer {
		for _, metric := range *metricSet{
			acc.AddFields(metric.Name(), metric.Fields(), metric.Tags(), metric.Time())
		}
	}

	c.metricsSetBuffer = nil
	return nil
}

func (c *ConfluentKafka) Start(acc telegraf.Accumulator) error {

	c.Log.Error("START")

	return nil
}

func (c *ConfluentKafka) Consume() {

	c.consuming = true
	
	for {

		ev := c.consumer.Poll(0)
		switch m := ev.(type) {

		case *kafka.Message:

			c.Log.Debugf("%% Message on %s:\n%s\n", m.TopicPartition, string(m.Value))

			metricsSet, err := c.parser.Parse(m.Value)
			if err != nil {
				c.Log.Errorf("Message parsing error: %v\n", err)
			}
			c.metricsSetBuffer = append(c.metricsSetBuffer, &metricsSet)


		case kafka.Error:
			c.Log.Errorf("Error: %v\n", m)
		}
	}
}


func init() {
	inputs.Add("confluent_cloud", func() telegraf.Input { 
		return &ConfluentKafka{}
	})
}