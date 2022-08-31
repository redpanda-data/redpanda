// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package utils

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"text/template"

	"github.com/Masterminds/sprig/v3"
)

// validEndpointRegexp checks that the endpoint is a valid hostname segment
var validEndpointRegexp = regexp.MustCompile(`^([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])$`)

// EndpointTemplateData provides data necessary to fill endpoint templates.
type EndpointTemplateData struct {
	// Index is the Redpanda broker progressive number.
	Index int
	// HostIP is the underlying host IP address. It should contain the
	// value of hostIP that is also available in pod status (status.hostIP,
	// available also in Kubernetes downward API).
	HostIP string
}

// NewEndpointTemplateData creates endpoint template data with all required fields.
func NewEndpointTemplateData(index int, hostIP string) EndpointTemplateData {
	return EndpointTemplateData{
		Index:  index,
		HostIP: hostIP,
	}
}

// ComputeEndpoint constructs the expected endpoint name using the given
// template.
// In case the template is empty, the legacy method for computing the endpoint
// name is used, which consists in using the plain index.
func ComputeEndpoint(tmpl string, data EndpointTemplateData) (string, error) {
	if tmpl == "" {
		return strconv.Itoa(data.Index), nil
	}
	t, err := template.New("endpoint").Funcs(sprig.HermeticTxtFuncMap()).Parse(tmpl)
	if err != nil {
		return "", fmt.Errorf("could not parse template %q: %w", tmpl, err)
	}
	var b strings.Builder
	err = t.Execute(&b, data)
	if err != nil {
		return "", fmt.Errorf("could not process template %q with data %v: %w", tmpl, data, err)
	}

	ep := b.String()
	if !validEndpointRegexp.MatchString(ep) {
		return "", &InvalidEndpointSegmentError{endpoint: ep}
	}

	return ep, nil
}

// InvalidEndpointSegmentError indicates that the generated endpoint is not a
// valid hostname segment
type InvalidEndpointSegmentError struct {
	endpoint string
}

// Error gives a string representation of the error
func (e *InvalidEndpointSegmentError) Error() string {
	return fmt.Sprintf("computed endpoint %s is not a valid hostname segment according to regexp: %s", e.endpoint, validEndpointRegexp.String())
}
