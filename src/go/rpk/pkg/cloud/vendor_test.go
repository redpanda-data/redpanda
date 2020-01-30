package cloud

import (
	"fmt"
	"testing"
)

type mockVendor struct {
	available bool
	name      string
	vmType    string
}

func (v *mockVendor) Name() string {
	return v.name
}

func (v *mockVendor) Init() (InitializedVendor, error) {
	if !v.available {
		return nil, fmt.Errorf("mockVendor '%s' is not available", v.name)
	}
	return v, nil
}

func (v *mockVendor) VmType() (string, error) {
	return v.vmType, nil
}

func TestAvailableVendor(t *testing.T) {
	var (
		name1 = "vendor1"
		name2 = "vendor2"
		name3 = "vendor3"
	)
	vendors := make(map[string]Vendor)
	vendors[name1] = &mockVendor{false, name1, ""}
	vendors[name2] = &mockVendor{true, name2, ""}
	vendors[name3] = &mockVendor{false, name3, ""}

	availableVendor, err := availableVendorFrom(vendors)

	if err != nil {
		t.Errorf("got an unexpected error: %v", err)
	}

	if availableVendor.Name() != name2 {
		t.Errorf("expected vendor '%s', got '%s'", name2, availableVendor.Name())
	}
}

func TestUnvailableVendor(t *testing.T) {
	var (
		name1 = "vendor1"
		name2 = "vendor2"
		name3 = "vendor3"
	)
	vendors := make(map[string]Vendor)
	vendors[name1] = &mockVendor{false, name1, ""}
	vendors[name2] = &mockVendor{false, name2, ""}
	vendors[name3] = &mockVendor{false, name3, ""}

	_, err := availableVendorFrom(vendors)

	if err == nil {
		t.Error("expected an error, but got nil")
	}
}
