package topic

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockMigrationClient is a mock implementation of the migrationClient interface
type MockMigrationClient struct {
	mock.Mock
}

func (m *MockMigrationClient) ExecuteMigration(ctx context.Context, id int, action rpadmin.MigrationAction) error {
	args := m.Called(ctx, id, action)
	return args.Error(0)
}

func (m *MockMigrationClient) GetMigration(ctx context.Context, id int) (rpadmin.MigrationState, error) {
	args := m.Called(ctx, id)
	return args.Get(0).(rpadmin.MigrationState), args.Error(1)
}

func TestCheckMigrationActionAndAdvanceState(t *testing.T) {
	tests := []struct {
		name           string
		id             int
		action         rpadmin.MigrationAction
		desiredStatus  rpadmin.MigrationStatus
		timeout        time.Duration
		executionError error
		getMigrations  []rpadmin.MigrationState
		getErrors      []error
		expectedError  string
	}{
		{
			name:           "Prepare Migration Success",
			id:             1,
			action:         rpadmin.MigrationActionPrepare,
			desiredStatus:  rpadmin.MigrationStatusPrepared,
			timeout:        11 * time.Second,
			executionError: nil,
			getMigrations:  []rpadmin.MigrationState{{State: "preparing"}, {State: "prepared"}},
			getErrors:      []error{nil, nil},
			expectedError:  "",
		},
		{
			name:           "Execute Migration Success",
			id:             2,
			action:         rpadmin.MigrationActionExecute,
			desiredStatus:  rpadmin.MigrationStatusExecuted,
			timeout:        11 * time.Second,
			executionError: nil,
			getMigrations:  []rpadmin.MigrationState{{State: "executing"}, {State: "executed"}},
			getErrors:      []error{nil, nil},
			expectedError:  "",
		},
		{
			name:           "Finish Migration Success",
			id:             3,
			action:         rpadmin.MigrationActionFinish,
			desiredStatus:  rpadmin.MigrationStatusFinished,
			timeout:        11 * time.Second,
			executionError: nil,
			getMigrations:  []rpadmin.MigrationState{{State: "finishing"}, {State: "finished"}},
			getErrors:      []error{nil, nil},
			expectedError:  "",
		},
		{
			name:           "Execution Error",
			id:             4,
			action:         rpadmin.MigrationActionPrepare,
			desiredStatus:  rpadmin.MigrationStatusPrepared,
			timeout:        10 * time.Second,
			executionError: errors.New("execution failed"),
			getMigrations:  []rpadmin.MigrationState{},
			getErrors:      []error{},
			expectedError:  "unable to execute migration: execution failed",
		},
		{
			name:           "Get Migration Error",
			id:             5,
			action:         rpadmin.MigrationActionExecute,
			desiredStatus:  rpadmin.MigrationStatusExecuted,
			timeout:        10 * time.Second,
			executionError: nil,
			getMigrations:  []rpadmin.MigrationState{{State: "executing"}},
			getErrors:      []error{errors.New("get migration failed")},
			expectedError:  "unable to get migration state: get migration failed",
		},
		{
			name:           "Timeout",
			id:             6,
			action:         rpadmin.MigrationActionFinish,
			desiredStatus:  rpadmin.MigrationStatusFinished,
			timeout:        16 * time.Second,
			executionError: nil,
			getMigrations:  []rpadmin.MigrationState{{State: "finishing"}, {State: "finishing"}, {State: "finishing"}},
			getErrors:      []error{nil, nil, nil},
			expectedError:  "operation timed out: context deadline exceeded",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a context with timeout
			ctx, cancel := context.WithTimeout(context.Background(), tt.timeout)
			defer cancel() // Ensure the context is canceled when we're done

			mockClient := new(MockMigrationClient)

			// Use a custom matcher for context
			contextMatcher := mock.MatchedBy(func(c context.Context) bool {
				// Check if the context has a deadline (timeout)
				deadline, hasDeadline := c.Deadline()
				// Check if the context can be canceled
				select {
				case <-c.Done():
					return false // Context is already done, which is not what we expect
				default:
					// Context is not done, which is what we expect
				}
				return hasDeadline && time.Until(deadline) <= tt.timeout
			})

			mockClient.On("ExecuteMigration", contextMatcher, tt.id, tt.action).Return(tt.executionError)

			for i := range tt.getMigrations {
				mockClient.On("GetMigration", contextMatcher, tt.id).Return(tt.getMigrations[i], tt.getErrors[i]).Once()
			}

			err := checkMigrationActionAndAdvanceState(ctx, tt.id, mockClient, tt.action, tt.desiredStatus, tt.timeout)

			if tt.expectedError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				assert.NoError(t, err)
			}

			mockClient.AssertExpectations(t)
		})
	}
}
