package confluent-kafka

import (
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/input"
)

type ConfluentKafka struct {

}

func (s *ConfluentKafka) SampleConfig() string {
	return ""
}

func (s *ConfluentKafka) Description() string {
	return "Sample Desc"
}

func (s *ConfluentKafka) Gather(acc telegraf.Accumulator) error {
	return nil
}

func init() {
	inputs.Add("confluent-kafka", func() telegraf.Input { return &ConfluentKafka{} })
}