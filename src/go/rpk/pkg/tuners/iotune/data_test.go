package iotune_test

import (
	"testing"
	"vectorized/pkg/tuners/iotune"
)

func TestDataFor(t *testing.T) {
	type args struct {
		vendor  string
		vm      string
		storage string
	}
	tests := []struct {
		name string
		args
		expectedErrMsg string
	}{
		{
			name: "it shouldn't fail for supported setups",
			args: args{"aws", "i3.large", "default"},
		},
		{
			name:           "it should return an error for unsupported vendors",
			args:           args{"unsupported", "i3.large", "default"},
			expectedErrMsg: "no iotune data found for vendor 'unsupported'",
		},
		{
			name:           "it should return an error for unsupported vms",
			args:           args{"aws", "unsupported", "default"},
			expectedErrMsg: "no iotune data found for VM 'unsupported', of vendor 'aws'",
		},
		{
			name:           "it should return an error for unsupported storage",
			args:           args{"aws", "i3.large", "unsupported"},
			expectedErrMsg: "no iotune data found for storage 'unsupported' in VM 'i3.large', of vendor 'aws'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := iotune.DataFor("/mount/point", tt.vendor, tt.vm, tt.storage)
			if tt.expectedErrMsg != "" {
				if err == nil {
					t.Errorf("Expected error with message '%s', got nil", tt.expectedErrMsg)
				}
				if err.Error() != tt.expectedErrMsg {
					t.Errorf("Expected error message:\n'%s'\ngot:\n'%s'", tt.expectedErrMsg, err.Error())
				}
			} else if err != nil {
				t.Errorf(
					"got an error for a supported setup ('%s', '%s', '%s'): %v",
					tt.vendor,
					tt.vm,
					tt.storage,
					err,
				)
			}
		})
	}
}
