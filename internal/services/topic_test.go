//
// Copyright Strimzi authors.
// License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
//

//go:build unit_test

// Package services defines an interface for canary services and related implementations

package services

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"
	"unsafe"

	"github.com/IBM/sarama"
	"github.com/taekjaskyli/kafka-canary/internal/config"
)

func TestRequestedAssignments(t *testing.T) {
	var tests = []struct {
		name                       string
		numPartitions              int
		numBrokers                 int
		useRack                    bool
		brokersWithMultipleLeaders []int32
		expectedMinISR             int
	}{
		{"one broker", 1, 1, false, []int32{}, 1},
		{"three brokers without rack info", 3, 3, false, []int32{}, 2},
		{"fewer brokers than partitions", 3, 2, false, []int32{0}, 1},
		{"six brokers with rack info", 6, 6, true, []int32{}, 2},
	}

	cfg := &config.CanaryConfig{
		Topic: "test",
	}
	ts := NewTopicService(cfg, nil).(*topicService)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			brokers, brokerMap := createBrokers(t, tt.numBrokers, tt.useRack)

			assignments, minISR := ts.requestedAssignments(tt.numPartitions, brokers)

			if tt.expectedMinISR != minISR {
				t.Errorf("unexpected minISR, got = %d, want = %d", minISR, tt.expectedMinISR)
			}

			for _, brokerIds := range assignments {
				duplicatedBrokers := make(map[int32]int)

				for _, brokerId := range brokerIds {
					if _, ok := duplicatedBrokers[brokerId]; !ok {
						duplicatedBrokers[brokerId] = 1
					} else {
						duplicatedBrokers[brokerId] = duplicatedBrokers[brokerId] + 1
					}

				}

				for brokerId, count := range duplicatedBrokers {
					if count > 1 {
						t.Errorf("partition is replicated on same broker (%d) more than once (%d)", brokerId, count)
					}
				}
			}

			leaderBrokers := make(map[int32]int)
			for _, brokerIds := range assignments {

				leaderBrokerId := brokerIds[0]
				if _, ok := leaderBrokers[leaderBrokerId]; !ok {
					leaderBrokers[leaderBrokerId] = 1
				} else {
					leaderBrokers[leaderBrokerId] = leaderBrokers[leaderBrokerId] + 1
				}
			}

			for brokerId, count := range leaderBrokers {
				if count > 1 {
					found := false
					for _, expectedBrokerId := range tt.brokersWithMultipleLeaders {
						if expectedBrokerId == brokerId {
							found = true
							break
						}
					}

					if !found {
						t.Errorf("broker %d is leader for more than one partition (%d)", brokerId, count)
					}
				}
			}

			if tt.useRack {
				for i, brokerIds := range assignments {
					rackBrokerId := make(map[string][]int32)
					for _, brokerId := range brokerIds {
						broker := brokerMap[brokerId]
						_, ok := rackBrokerId[broker.Rack()]
						if !ok {
							rackBrokerId[broker.Rack()] = make([]int32, 0)
						}
						rackBrokerId[broker.Rack()] = append(rackBrokerId[broker.Rack()], broker.ID())
					}

					for rack, brokerIds := range rackBrokerId {
						if len(brokerIds) > 1 {
							t.Errorf("partition %d has been assigned to %d brokers %v in rackBrokerId %s", i, len(brokerIds), brokerIds, rack)
						}
					}
				}
			}

			ts.Close()
		})
	}

}

func createBrokers(t *testing.T, num int, rack bool) ([]*sarama.Broker, map[int32]*sarama.Broker) {
	brokers := make([]*sarama.Broker, 0)
	brokerMap := make(map[int32]*sarama.Broker)
	for i := 0; i < num; i++ {
		broker := &sarama.Broker{}

		setBrokerID(t, broker, i)

		brokers = append(brokers, broker)
		brokerMap[broker.ID()] = broker
	}

	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(brokers), func(i, j int) { brokers[i], brokers[j] = brokers[j], brokers[i] })

	if rack {
		rackNames := make([]string, 3)
		for i, _ := range rackNames {
			rackNames[i] = fmt.Sprintf("useRack%d", i)
		}

		for i, broker := range brokers {
			rackName := rackNames[i%3]
			setBrokerRack(t, broker, rackName)
		}
	}

	return brokers, brokerMap
}

func setBrokerID(t *testing.T, broker *sarama.Broker, i int) {
	idV := reflect.ValueOf(broker).Elem().FieldByName("id")
	setUnexportedField(idV, int32(i))
	if int32(i) != broker.ID() {
		t.Errorf("failed to set id by reflection")
	}
}

func setBrokerRack(t *testing.T, broker *sarama.Broker, rackName string) {
	rackV := reflect.ValueOf(broker).Elem().FieldByName("rack")
	setUnexportedField(rackV, &rackName)
	if rackName != broker.Rack() {
		t.Errorf("failed to set useRack by reflection")
	}
}

func setUnexportedField(field reflect.Value, value interface{}) {
	reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).
		Elem().
		Set(reflect.ValueOf(value))
}

func existingTopicMetadata(name string) *sarama.TopicMetadata {
	return &sarama.TopicMetadata{
		Name: name,
		Err:  sarama.ErrNoError,
		Partitions: []*sarama.PartitionMetadata{
			{ID: 0, Leader: 0, Replicas: []int32{0, 1}, Isr: []int32{0, 1}},
			{ID: 1, Leader: 1, Replicas: []int32{1, 0}, Isr: []int32{1, 0}},
		},
	}
}

func TestReconcileManageTopicFalseUsesExistingAssignments(t *testing.T) {
	topic := "kafka-canary"
	admin := &mockClusterAdmin{topicMeta: existingTopicMetadata(topic)}
	cfg := &config.CanaryConfig{
		Topic:               topic,
		ManageTopic:         false,
		TopicConfig:         map[string]string{"retention.ms": "600000"},
		ExpectedClusterSize: config.ExpectedClusterSizeDefault,
	}
	ts := NewTopicService(cfg, nil).(*topicService)
	ts.admin = admin

	result, err := ts.Reconcile()
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if admin.describeClusterCalls != 0 || admin.createTopicCalls != 0 || admin.alterConfigCalls != 0 || admin.incrementalAlterConfigCalls != 0 || admin.alterReassignCalls != 0 || admin.createPartitionsCalls != 0 {
		t.Fatalf("create=%d alterConfig=%d incrementalAlter=%d reassign=%d createPartitions=%d describeCluster=%d",
			admin.createTopicCalls, admin.alterConfigCalls, admin.incrementalAlterConfigCalls, admin.alterReassignCalls, admin.createPartitionsCalls, admin.describeClusterCalls)
	}
	if len(result.Assignments[0]) != 2 || result.Assignments[0][0] != 0 {
		t.Fatalf("assignments = %v", result.Assignments)
	}
	if result.Leaders[0] != 0 || result.Leaders[1] != 1 {
		t.Fatalf("leaders = %v", result.Leaders)
	}
}

func TestReconcileManageTopicFalseMissingTopic(t *testing.T) {
	topic := "kafka-canary"
	admin := &mockClusterAdmin{
		topicMeta: &sarama.TopicMetadata{Name: topic, Err: sarama.ErrUnknownTopicOrPartition},
	}
	cfg := &config.CanaryConfig{Topic: topic, ManageTopic: false}
	ts := NewTopicService(cfg, nil).(*topicService)
	ts.admin = admin

	_, err := ts.Reconcile()
	var missing *ErrCanaryTopicMissing
	if !errors.As(err, &missing) {
		t.Fatalf("got %v, want ErrCanaryTopicMissing", err)
	}
	if missing.Topic != topic {
		t.Fatalf("topic = %s", missing.Topic)
	}
	if admin.createTopicCalls != 0 {
		t.Fatalf("CreateTopic called")
	}
}

func TestReconcileManageTopicTrueAltersConfig(t *testing.T) {
	topic := "kafka-canary"
	brokers, _ := createBrokers(t, 1, false)
	admin := &mockClusterAdmin{
		topicMeta: existingTopicMetadata(topic),
		brokers:   brokers,
	}
	cfg := &config.CanaryConfig{
		Topic:               topic,
		ManageTopic:         true,
		TopicConfig:         map[string]string{"retention.ms": "600000"},
		ExpectedClusterSize: 3,
	}
	ts := NewTopicService(cfg, nil).(*topicService)
	ts.admin = admin

	if _, err := ts.Reconcile(); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if admin.describeClusterCalls != 1 {
		t.Fatalf("DescribeCluster calls = %d", admin.describeClusterCalls)
	}
	if admin.incrementalAlterConfigCalls != 1 {
		t.Fatalf("IncrementalAlterConfig calls = %d", admin.incrementalAlterConfigCalls)
	}
	if admin.alterConfigCalls != 0 {
		t.Fatalf("AlterConfig calls = %d", admin.alterConfigCalls)
	}
	if admin.alterReassignCalls != 0 || admin.createTopicCalls != 0 {
		t.Fatalf("create=%d reassign=%d", admin.createTopicCalls, admin.alterReassignCalls)
	}
}
