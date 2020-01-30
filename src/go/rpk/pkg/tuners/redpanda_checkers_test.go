package tuners_test

import (
	"testing"
	"time"
	"vectorized/pkg/tuners"

	"github.com/spf13/afero"
)

func TestNtpCheckTimeout(t *testing.T) {
	timeout := time.Duration(0)

	check := tuners.NewNTPSyncChecker(timeout, afero.NewMemMapFs())

	res := check.Check()

	if res.IsOk {
		t.Errorf("the NTP check shouldn't have succeeded")
	}
	if res.Err == nil {
		t.Errorf("the NTP check should have failed with an error")
	}
}
