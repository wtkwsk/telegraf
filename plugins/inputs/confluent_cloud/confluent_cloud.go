package confluent_cloud

import (
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
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

	Log telegraf.Logger `toml:"-"`
}


func (c *ConfluentKafka) SampleConfig() string {
	return ConfluentKafkaConfig
}

func (c *ConfluentKafka) Description() string {
	return "Consume data from Confluent Cloud Kafka."
}

func(c *ConfluentKafka) Init() error {

	if c.SessionTimeout == 0 {
		c.SessionTimeout = defaultSessionTimeout
	}

	if c.AutoOffsetReset == "" {
		c.AutoOffsetReset = defaultAutoOffsetReset
	}

	return nil
}


func (c *ConfluentKafka) Gather(acc telegraf.Accumulator) error {

	fields := make(map[string]interface{})
	fields["test"] = 2.2

	tags := make(map[string]string)
	tags["id"] = "abc123"

	acc.AddFields("confluent_cloud", fields, tags)

	return nil
}


func init() {
	inputs.Add("confluent_cloud", func() telegraf.Input { 
		return &ConfluentKafka{}
	})
}