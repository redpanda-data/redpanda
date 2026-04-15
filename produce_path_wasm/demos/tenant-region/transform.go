package main

import (
	"fmt"
	"slices"

	"github.com/redpanda-data/redpanda/src/transform-sdk/go/transform"
)

// In production the tenant -> allowed-regions mapping would live in
// cluster config or be derived from OIDC group claims. Embedded here
// for demo clarity.
var tenantRegions = map[string][]string{
	"acme-us":     {"us"},
	"acme-eu":     {"eu"},
	"acme-global": {"us", "eu"},
}

// Listener -> region. Operator-chosen listener names, mapped to the
// residency region they service.
var listenerToRegion = map[string]string{
	"us-listener": "us",
	"eu-listener": "eu",
}

func main() {
	transform.OnRecordWritten(enforce)
}

func enforce(e transform.WriteEvent, w transform.RecordWriter) error {
	tenant := e.Metadata("principal_name")
	listener := e.Metadata("listener_name")

	region, ok := listenerToRegion[listener]
	if !ok {
		return fmt.Errorf("unknown listener %q", listener)
	}

	allowed, known := tenantRegions[tenant]
	if !known {
		return fmt.Errorf("unknown tenant %q", tenant)
	}

	if !slices.Contains(allowed, region) {
		return fmt.Errorf(
			"tenant %q not authorized for region %q (allowed: %v)",
			tenant, region, allowed)
	}

	rec := e.Record()
	rec.Headers = append(rec.Headers,
		transform.RecordHeader{Key: []byte("tenant"), Value: []byte(tenant)},
		transform.RecordHeader{Key: []byte("residency"), Value: []byte(region)},
	)
	return w.Write(rec)
}
