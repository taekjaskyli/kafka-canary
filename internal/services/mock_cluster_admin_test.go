//
// Copyright Strimzi authors.
// License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
//

//go:build unit_test

package services

import "github.com/IBM/sarama"

var _ sarama.ClusterAdmin = (*mockClusterAdmin)(nil)

type mockClusterAdmin struct {
	topicMeta                   *sarama.TopicMetadata
	brokers                     []*sarama.Broker
	createTopicCalls            int
	alterConfigCalls            int
	incrementalAlterConfigCalls int
	alterReassignCalls          int
	createPartitionsCalls       int
	describeClusterCalls        int
}

func (m *mockClusterAdmin) CreateTopic(string, *sarama.TopicDetail, bool) error {
	m.createTopicCalls++
	return nil
}
func (m *mockClusterAdmin) ListTopics() (map[string]sarama.TopicDetail, error) {
	return nil, nil
}
func (m *mockClusterAdmin) DescribeTopics([]string) ([]*sarama.TopicMetadata, error) {
	return []*sarama.TopicMetadata{m.topicMeta}, nil
}
func (m *mockClusterAdmin) DeleteTopic(string) error { return nil }
func (m *mockClusterAdmin) CreatePartitions(string, int32, [][]int32, bool) error {
	m.createPartitionsCalls++
	return nil
}
func (m *mockClusterAdmin) AlterPartitionReassignments(string, [][]int32) error {
	m.alterReassignCalls++
	return nil
}
func (m *mockClusterAdmin) ListPartitionReassignments(string, []int32) (map[string]map[int32]*sarama.PartitionReplicaReassignmentsStatus, error) {
	return nil, nil
}
func (m *mockClusterAdmin) DeleteRecords(string, map[int32]int64) error { return nil }
func (m *mockClusterAdmin) DescribeConfig(sarama.ConfigResource) ([]sarama.ConfigEntry, error) {
	return nil, nil
}
func (m *mockClusterAdmin) DescribeConfigs([]*sarama.ConfigResource, sarama.DescribeConfigsOptions) ([]*sarama.ConfigResourceResult, error) {
	return nil, nil
}
func (m *mockClusterAdmin) AlterConfig(sarama.ConfigResourceType, string, map[string]*string, bool) error {
	m.alterConfigCalls++
	return nil
}
func (m *mockClusterAdmin) IncrementalAlterConfig(sarama.ConfigResourceType, string, map[string]sarama.IncrementalAlterConfigsEntry, bool) error {
	m.incrementalAlterConfigCalls++
	return nil
}
func (m *mockClusterAdmin) CreateACL(sarama.Resource, sarama.Acl) error { return nil }
func (m *mockClusterAdmin) CreateACLs([]*sarama.ResourceAcls) error     { return nil }
func (m *mockClusterAdmin) ListAcls(sarama.AclFilter) ([]sarama.ResourceAcls, error) {
	return nil, nil
}
func (m *mockClusterAdmin) DeleteACL(sarama.AclFilter, bool) ([]sarama.MatchingAcl, error) {
	return nil, nil
}
func (m *mockClusterAdmin) ElectLeaders(sarama.ElectionType, map[string][]int32) (map[string]map[int32]*sarama.PartitionResult, error) {
	return nil, nil
}
func (m *mockClusterAdmin) ListConsumerGroups() (map[string]string, error) { return nil, nil }
func (m *mockClusterAdmin) DescribeConsumerGroups([]string) ([]*sarama.GroupDescription, error) {
	return nil, nil
}
func (m *mockClusterAdmin) ListConsumerGroupOffsets(string, map[string][]int32) (*sarama.OffsetFetchResponse, error) {
	return nil, nil
}
func (m *mockClusterAdmin) ListConsumerGroupOffsetsBatch(map[string]map[string][]int32) (map[string]*sarama.OffsetFetchResponseGroup, error) {
	return nil, nil
}
func (m *mockClusterAdmin) ListOffsets(map[string]map[int32]int64, *sarama.ListOffsetsOptions) (map[string]map[int32]*sarama.OffsetResult, error) {
	return nil, nil
}
func (m *mockClusterAdmin) AlterConsumerGroupOffsets(string, map[string]map[int32]sarama.OffsetAndMetadata, *sarama.AlterConsumerGroupOffsetsOptions) (*sarama.OffsetCommitResponse, error) {
	return nil, nil
}
func (m *mockClusterAdmin) DeleteConsumerGroupOffset(string, string, int32) error { return nil }
func (m *mockClusterAdmin) DeleteConsumerGroup(string) error                      { return nil }
func (m *mockClusterAdmin) DescribeCluster() ([]*sarama.Broker, int32, error) {
	m.describeClusterCalls++
	return m.brokers, 0, nil
}
func (m *mockClusterAdmin) DescribeLogDirs([]int32) (map[int32][]sarama.DescribeLogDirsResponseDirMetadata, error) {
	return nil, nil
}
func (m *mockClusterAdmin) DescribeUserScramCredentials([]string) ([]*sarama.DescribeUserScramCredentialsResult, error) {
	return nil, nil
}
func (m *mockClusterAdmin) DeleteUserScramCredentials([]sarama.AlterUserScramCredentialsDelete) ([]*sarama.AlterUserScramCredentialsResult, error) {
	return nil, nil
}
func (m *mockClusterAdmin) UpsertUserScramCredentials([]sarama.AlterUserScramCredentialsUpsert) ([]*sarama.AlterUserScramCredentialsResult, error) {
	return nil, nil
}
func (m *mockClusterAdmin) UpdateFeatures([]sarama.FeatureUpdate) ([]sarama.UpdatableFeatureResult, error) {
	return nil, nil
}
func (m *mockClusterAdmin) DescribeClientQuotas([]sarama.QuotaFilterComponent, bool) ([]sarama.DescribeClientQuotasEntry, error) {
	return nil, nil
}
func (m *mockClusterAdmin) AlterClientQuotas([]sarama.QuotaEntityComponent, sarama.ClientQuotasOp, bool) error {
	return nil
}
func (m *mockClusterAdmin) Controller() (*sarama.Broker, error)        { return nil, nil }
func (m *mockClusterAdmin) Coordinator(string) (*sarama.Broker, error) { return nil, nil }
func (m *mockClusterAdmin) RemoveMemberFromConsumerGroup(string, []string) (*sarama.LeaveGroupResponse, error) {
	return nil, nil
}
func (m *mockClusterAdmin) Close() error { return nil }
