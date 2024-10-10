package adminapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func Test_licenseFeatureChecks(t *testing.T) {
	tests := []struct {
		name         string
		prof         *config.RpkProfile
		responseCase string // See the mapLicenseResponses below.
		expContain   string
		withErr      bool
		checkCache   func(t *testing.T, before int64, after int64)
	}{
		{
			name:         "license ok, first time call",
			prof:         &config.RpkProfile{},
			responseCase: "ok",
			expContain:   "",
		},
		{
			name: "license ok, cache valid",
			prof: &config.RpkProfile{LicenseCheck: &config.LicenseStatusCache{LastUpdate: time.Now().Add(20 * time.Minute).Unix()}},
			checkCache: func(t *testing.T, before int64, after int64) {
				// If the cache was valid, last update shouldn't have changed.
				require.Equal(t, before, after)
			},
			responseCase: "ok",
			expContain:   "",
		},
		{
			name: "license ok, old cache",
			prof: &config.RpkProfile{LicenseCheck: &config.LicenseStatusCache{LastUpdate: time.Now().AddDate(0, 0, -20).Unix()}}, // Limit is 1 hour
			checkCache: func(t *testing.T, before int64, after int64) {
				// Date should be updated.
				afterT := time.Unix(after, 0)
				require.True(t, time.Unix(before, 0).Before(afterT))
			},
			responseCase: "ok",
			expContain:   "",
		},
		{
			name:         "inViolation, first time call",
			prof:         &config.RpkProfile{},
			responseCase: "inViolation",
			expContain:   "These features require a license",
		},
		{
			name:         "inViolation, expired last check",
			prof:         &config.RpkProfile{LicenseCheck: &config.LicenseStatusCache{LastUpdate: time.Now().AddDate(0, 0, -20).Unix()}},
			responseCase: "inViolation",
			expContain:   "These features require a license",
		},
		{
			// Edge case when the license expires but the last check was less
			// than 1 hour ago.
			name:         "inViolation, cache still valid",
			prof:         &config.RpkProfile{LicenseCheck: &config.LicenseStatusCache{LastUpdate: time.Now().Add(30 * time.Minute).Unix()}},
			responseCase: "inViolation",
			// In this case, even if the license is in violation, rpk won't
			// reach the Admin API because the last check was under 15 days.
			checkCache: func(t *testing.T, before int64, after int64) {
				// At this point we don't rewrite the last check, because still
				// valid.
				require.Equal(t, before, after)
			},
			expContain: "",
		},
		{
			name:         "admin API errors, don't print",
			prof:         &config.RpkProfile{},
			withErr:      true,
			responseCase: "failedRequest",
			// If we fail to communicate with the cluster, or the request fails,
			// then we just WARN the user of what happened but won't print to
			// stdout.
			expContain: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := httptest.NewServer(licenseHandler(tt.responseCase))
			defer ts.Close()
			tt.prof.AdminAPI = config.RpkAdminAPI{Addresses: []string{ts.URL}}
			fs := afero.NewMemMapFs()
			loadedProfile := writeRpkProfileToFs(t, fs, tt.prof)
			client, err := NewHostClient(fs, loadedProfile, "0")
			require.NoError(t, err)
			got := licenseFeatureChecks(context.Background(), fs, client, loadedProfile)
			if tt.expContain == "" {
				require.Empty(t, got)
				if tt.withErr {
					return
				}
				// If we get to this point, we need to make sure that the last
				// update date was registered.
				afterProf := loadProfile(t, fs)
				require.NotEmpty(t, afterProf.LicenseCheck)
				require.NotEmpty(t, afterProf.LicenseCheck.LastUpdate)
				if tt.checkCache != nil {
					tt.checkCache(t, tt.prof.LicenseCheck.LastUpdate, afterProf.LicenseCheck.LastUpdate)
				}
				return
			}
			require.Contains(t, got, tt.expContain)
			// If we get to this point, then we shouldn't have the last
			// update registered.
			afterProf := loadProfile(t, fs)
			require.Empty(t, afterProf.LicenseCheck)
		})
	}
}

func writeRpkProfileToFs(t *testing.T, fs afero.Fs, p *config.RpkProfile) *config.RpkProfile {
	p.Name = "test"
	rpkyaml := config.RpkYaml{
		CurrentProfile: "test",
		Version:        6,
		Profiles:       []config.RpkProfile{*p},
	}
	err := rpkyaml.Write(fs)
	require.NoError(t, err)

	return loadProfile(t, fs)
}

func loadProfile(t *testing.T, fs afero.Fs) *config.RpkProfile {
	y, err := new(config.Params).Load(fs)
	require.NoError(t, err)
	return y.VirtualProfile()
}

type response struct {
	status int
	body   string
}

var mapLicenseResponses = map[string]response{
	"ok":            {http.StatusOK, `{"license_status": "valid", "violation": false}`},
	"inViolation":   {http.StatusOK, `{"license_status": "expired", "violation": true, "features": [{"name": "partition_auto_balancing_continuous", "enabled": true}]}`},
	"failedRequest": {http.StatusBadRequest, ""},
}

func licenseHandler(respCase string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		resp := mapLicenseResponses[respCase]
		w.WriteHeader(resp.status)
		w.Write([]byte(resp.body))
	}
}
