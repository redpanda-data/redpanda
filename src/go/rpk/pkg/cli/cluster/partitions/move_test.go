package partitions

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/stretchr/testify/require"
)

func Test_extractNTP(t *testing.T) {
	tests := []struct {
		name      string
		topic     string
		partition string
		want0     string
		want1     string
		want2     int
		expErr    bool
	}{
		{
			name:      "t/p:r,r,r",
			topic:     "",
			partition: "foo/0:1,2,3",
			want0:     "kafka",
			want1:     "foo",
			want2:     0,
		},
		{
			name:      "t/p:r,r,r",
			topic:     "",
			partition: "foo/0:1,2,3",
			want0:     "kafka",
			want1:     "foo",
			want2:     0,
		},
		{
			name:      "p:r,r,r",
			topic:     "foo",
			partition: "0:1,2,3",
			want0:     "",
			want1:     "",
			want2:     0,
		},
		{
			name:      "ns/t/p:r,r,r",
			topic:     "",
			partition: "redpanda_internal/tx/0:1,2,3",
			want0:     "redpanda_internal",
			want1:     "tx",
			want2:     0,
		},
		{
			name:      "t/p",
			topic:     "",
			partition: "foo/0",
			want2:     -1,
			expErr:    true,
		},
		{
			name:      "t/t/t:r,r,r",
			topic:     "",
			partition: "foo/bar/foo",
			want2:     -1,
			expErr:    true,
		},
		{
			name:      "topic t/p:r,r,r",
			topic:     "foo",
			partition: "bar/0:1,2,3",
			want2:     -1,
			expErr:    true,
		},
		{
			name:      "topic non-digit:r,r,r",
			topic:     "foo",
			partition: "one:1,2,3",
			want2:     -1,
			expErr:    true,
		},
		{
			name:      "t/non-digit:r,r,r",
			topic:     "",
			partition: "foo/one:1,2,3",
			want2:     -1,
			expErr:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got0, got1, got2, err := extractNTP(tt.topic, tt.partition)
			gotErr := err != nil
			if gotErr != tt.expErr {
				t.Errorf("got err? %v (%v), exp err? %v", gotErr, err, tt.expErr)
			}
			if got0 != tt.want0 {
				t.Errorf("extractNTP() got = %v, want %v", got0, tt.want0)
			}
			if got1 != tt.want1 {
				t.Errorf("extractNTP() got1 = %v, want %v", got1, tt.want1)
			}
			if got2 != tt.want2 {
				t.Errorf("extractNTP() got2 = %v, want %v", got2, tt.want2)
			}
		})
	}
}

func Test_extractReplicaChanges(t *testing.T) {
	tests := []struct {
		name            string
		partition       string
		currentReplica  rpadmin.Replicas
		expAllReplicas  rpadmin.Replicas
		expCoreReplicas rpadmin.Replicas
		expErrContain   string
	}{
		{
			name:      "only node changes",
			partition: "0:1,2,4",
			currentReplica: rpadmin.Replicas{
				{NodeID: 1, Core: 0},
				{NodeID: 2, Core: 1},
				{NodeID: 3, Core: 1},
			},
			expAllReplicas: rpadmin.Replicas{
				{NodeID: 1, Core: 0},
				{NodeID: 2, Core: 1},
				{NodeID: 4, Core: -1}, // new node
			},
		},
		{
			name:      "only core changes",
			partition: "myTopic/1:1-1,2-3,3-4",
			currentReplica: rpadmin.Replicas{
				{NodeID: 1, Core: 3},
				{NodeID: 2, Core: 1},
				{NodeID: 3, Core: 2},
			},
			expAllReplicas: rpadmin.Replicas{
				{NodeID: 1, Core: 1},
				{NodeID: 2, Core: 3},
				{NodeID: 3, Core: 4},
			},
			expCoreReplicas: rpadmin.Replicas{
				{NodeID: 1, Core: 1},
				{NodeID: 2, Core: 3},
				{NodeID: 3, Core: 4},
			},
		},
		{
			name:      "mixed changes",
			partition: "kafka/cipot/1:1,2-3,3",
			currentReplica: rpadmin.Replicas{
				{NodeID: 1, Core: 3},
				{NodeID: 2, Core: 1},
				{NodeID: 3, Core: 2},
			},
			expAllReplicas: rpadmin.Replicas{
				{NodeID: 1, Core: 3},
				{NodeID: 2, Core: 3},
				{NodeID: 3, Core: 2},
			},
			expCoreReplicas: rpadmin.Replicas{
				{NodeID: 2, Core: 3},
			},
		},
		{
			name:      "core changes, but already exist (no changes)",
			partition: "kafka/cipot/1:1-1,2-2,3-3",
			currentReplica: rpadmin.Replicas{
				{NodeID: 1, Core: 1},
				{NodeID: 2, Core: 2},
				{NodeID: 3, Core: 3},
			},
			expAllReplicas: rpadmin.Replicas{
				{NodeID: 1, Core: 1},
				{NodeID: 2, Core: 2},
				{NodeID: 3, Core: 3},
			},
			expCoreReplicas: nil,
		},
		{
			name:      "core changes in new node (-1)",
			partition: "3:1-2,2-1,4-3", // 4 is the new node.
			currentReplica: rpadmin.Replicas{
				{NodeID: 1, Core: 1},
				{NodeID: 2, Core: 2},
				{NodeID: 3, Core: 3},
			},
			expErrContain: "this command does not support updating cores for replicas on new nodes",
		},
		{
			name:      "replication factor change",
			partition: "3:1-2", // from 3 to 1
			currentReplica: rpadmin.Replicas{
				{NodeID: 1, Core: 1},
				{NodeID: 2, Core: 2},
				{NodeID: 3, Core: 3},
			},
			expErrContain: "cannot modify replication factor",
		},
		{
			name:          "invalid partition partition format",
			partition:     "3:1-2-3",
			expErrContain: "invalid format",
		},
		{
			name:          "invalid partition ntp format",
			partition:     "1-2,3,2",
			expErrContain: "invalid format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			all, core, err := extractReplicaChanges(tt.partition, tt.currentReplica)
			if tt.expErrContain != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.expErrContain)
				return
			}
			require.NoError(t, err)

			require.Equal(t, all, tt.expAllReplicas)
			require.Equal(t, core, tt.expCoreReplicas)
		})
	}
}

func Test_areReplicasEqual(t *testing.T) {
	tests := []struct {
		name string
		a    rpadmin.Replicas
		b    rpadmin.Replicas
		exp  bool
	}{
		{
			a:   rpadmin.Replicas{},
			b:   rpadmin.Replicas{},
			exp: true,
		},
		{
			a:   rpadmin.Replicas{{NodeID: 1, Core: 3}},
			b:   rpadmin.Replicas{{NodeID: 1, Core: 3}},
			exp: true,
		},
		{
			a:   rpadmin.Replicas{{NodeID: 1, Core: 3}, {NodeID: 2, Core: 1}},
			b:   rpadmin.Replicas{{NodeID: 2, Core: 1}, {NodeID: 1, Core: 3}},
			exp: true,
		},
		{
			a:   rpadmin.Replicas{{NodeID: 1, Core: 3}, {NodeID: 2, Core: 1}},
			b:   rpadmin.Replicas{{NodeID: 2, Core: 1}, {NodeID: 1, Core: 2}},
			exp: false,
		},
		{
			a: rpadmin.Replicas{}, b: nil, exp: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.exp, areReplicasEqual(tt.a, tt.b))
		})
	}
}

func Test_fillAssignmentList(t *testing.T) {
	tests := []struct {
		name      string
		coreCount int
		list      []newAssignment
	}{
		{
			name:      "fill all",
			coreCount: 3,
			list: []newAssignment{
				{
					NewReplicas: rpadmin.Replicas{
						{NodeID: 1, Core: 1},
						{NodeID: 2, Core: -1},
						{NodeID: 3, Core: 3},
					},
				},
				{
					NewReplicas: rpadmin.Replicas{
						{NodeID: 1, Core: -1},
						{NodeID: 2, Core: -1},
						{NodeID: 3, Core: -1},
					},
				},
				{
					NewReplicas: rpadmin.Replicas{
						{NodeID: 1, Core: 2},
						{NodeID: 2, Core: 1},
						{NodeID: 3, Core: 3},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			ts := httptest.NewServer(brokerHandler(tt.coreCount))
			defer ts.Close()
			cl, err := rpadmin.NewClient([]string{ts.URL}, nil, new(rpadmin.NopAuth), false)
			require.NoError(t, err)

			got, err := fillAssignmentList(ctx, cl, tt.list)
			require.NoError(t, err)
			for _, a := range got {
				for _, r := range a.NewReplicas {
					// We can't check for an exact value because
					// fillAssignmentList assigns a random core.
					require.NotEqual(t, -1, r.Core)
				}
			}
		})
	}
}

func brokerHandler(numCores int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		resp := fmt.Sprintf(`{"num_cores":%v}`, numCores)
		w.Write([]byte(resp))
	}
}

func Test_parseAssignments(t *testing.T) {
	tests := []struct {
		name                  string
		partitionsFlag        []string
		topics                []string
		currentReplicas       rpadmin.Replicas
		expNodeAssignmentList []newAssignment
		expCoreAssignmentList []newAssignment
		expErrContains        string
	}{
		{
			name:           "node and core changes, topic in partition flag",
			partitionsFlag: []string{"foo/0:4,2-1,3-0"},
			currentReplicas: rpadmin.Replicas{
				{NodeID: 1, Core: 0},
				{NodeID: 2, Core: 2},
				{NodeID: 3, Core: 4},
			},
			expNodeAssignmentList: []newAssignment{
				{
					Namespace: "kafka",
					Topic:     "foo",
					Partition: 0,
					OldReplicas: rpadmin.Replicas{
						{NodeID: 1, Core: 0},
						{NodeID: 2, Core: 2},
						{NodeID: 3, Core: 4},
					},
					NewReplicas: rpadmin.Replicas{
						{NodeID: 2, Core: 1},
						{NodeID: 3, Core: 0},
						{NodeID: 4, Core: -1},
					},
				},
			},
			expCoreAssignmentList: []newAssignment{
				{
					Namespace: "kafka",
					Topic:     "foo",
					Partition: 0,
					OldReplicas: rpadmin.Replicas{
						{NodeID: 1, Core: 0},
						{NodeID: 2, Core: 2},
						{NodeID: 3, Core: 4},
					},
					NewReplicas: rpadmin.Replicas{
						{NodeID: 2, Core: 1},
						{NodeID: 3, Core: 0},
					},
				},
			},
		},
		{
			name:           "multiple changes, topics in partition flag",
			partitionsFlag: []string{"foo/0:4,2-1,3-0", "kafka_internal/bar/0:1-2,2-4,3"},
			currentReplicas: rpadmin.Replicas{
				{NodeID: 1, Core: 0},
				{NodeID: 2, Core: 2},
				{NodeID: 3, Core: 4},
			},
			expNodeAssignmentList: []newAssignment{
				{
					Namespace: "kafka",
					Topic:     "foo",
					Partition: 0,
					OldReplicas: rpadmin.Replicas{
						{NodeID: 1, Core: 0},
						{NodeID: 2, Core: 2},
						{NodeID: 3, Core: 4},
					},
					NewReplicas: rpadmin.Replicas{
						{NodeID: 2, Core: 1},
						{NodeID: 3, Core: 0},
						{NodeID: 4, Core: -1},
					},
				},
				{
					Namespace: "kafka_internal",
					Topic:     "bar",
					Partition: 0,
					OldReplicas: rpadmin.Replicas{
						{NodeID: 1, Core: 0},
						{NodeID: 2, Core: 2},
						{NodeID: 3, Core: 4},
					},
					NewReplicas: rpadmin.Replicas{
						{NodeID: 1, Core: 2},
						{NodeID: 2, Core: 4},
						{NodeID: 3, Core: 4},
					},
				},
			},
			expCoreAssignmentList: []newAssignment{
				{
					Namespace: "kafka",
					Topic:     "foo",
					Partition: 0,
					OldReplicas: rpadmin.Replicas{
						{NodeID: 1, Core: 0},
						{NodeID: 2, Core: 2},
						{NodeID: 3, Core: 4},
					},
					NewReplicas: rpadmin.Replicas{
						{NodeID: 2, Core: 1},
						{NodeID: 3, Core: 0},
					},
				},
				{
					Namespace: "kafka_internal",
					Topic:     "bar",
					Partition: 0,
					OldReplicas: rpadmin.Replicas{
						{NodeID: 1, Core: 0},
						{NodeID: 2, Core: 2},
						{NodeID: 3, Core: 4},
					},
					NewReplicas: rpadmin.Replicas{
						{NodeID: 1, Core: 2},
						{NodeID: 2, Core: 4},
					},
				},
			},
		},
		{
			name:           "no node and no core changes, topic in args",
			partitionsFlag: []string{"0:1,2-1,3-0"}, // The same as the current replicas
			topics:         []string{"foo"},
			currentReplicas: rpadmin.Replicas{
				{NodeID: 1, Core: 0},
				{NodeID: 2, Core: 1},
				{NodeID: 3, Core: 0},
			},
		},
		{
			name:           "err: replication change",
			partitionsFlag: []string{"kafka/myTopic/0:1,2-1"},
			currentReplicas: rpadmin.Replicas{
				{NodeID: 1, Core: 0},
				{NodeID: 2, Core: 1},
				{NodeID: 3, Core: 0},
			},
			expErrContains: "cannot modify replication factor",
		},
		{
			name:           "err: core change on new node",
			partitionsFlag: []string{"kafka/myTopic/0:1,2-3,4-2"},
			currentReplicas: rpadmin.Replicas{
				{NodeID: 1, Core: 0},
				{NodeID: 2, Core: 1},
				{NodeID: 3, Core: 0},
			},
			expErrContains: "does not support updating cores for replicas on new nodes",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			ts := httptest.NewServer(partitionHandler(tt.currentReplicas))
			defer ts.Close()
			cl, err := rpadmin.NewClient([]string{ts.URL}, nil, new(rpadmin.NopAuth), false)
			require.NoError(t, err)

			gotNodeAssignmentList, gotCoreAssignmentList, err := parseAssignments(ctx, cl, tt.partitionsFlag, tt.topics)
			if tt.expErrContains != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.expErrContains)
				return
			}
			require.NoError(t, err)

			require.Equal(t, tt.expNodeAssignmentList, gotNodeAssignmentList)
			require.Equal(t, tt.expCoreAssignmentList, gotCoreAssignmentList)
		})
	}
}

func partitionHandler(replicas rpadmin.Replicas) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		partition := rpadmin.Partition{
			Replicas: replicas,
		}
		resp, _ := json.Marshal(partition)
		w.Write(resp)
	}
}
