package cloud

import (
	"errors"
	"sync"

	log "github.com/sirupsen/logrus"
)

type Vendor interface {
	Name() string
	Init() (InitializedVendor, error)
}

type InitializedVendor interface {
	Name() string
	VmType() (string, error)
}

func vendors() map[string]Vendor {
	vendors := make(map[string]Vendor)
	aws := &AwsVendor{}
	vendors[aws.Name()] = aws

	return vendors
}

// Tries to initializes the vendors and returns the one available, or an error
// if none could be initialized.
func AvailableVendor() (InitializedVendor, error) {
	return availableVendorFrom(vendors())
}

func availableVendorFrom(vendors map[string]Vendor) (InitializedVendor, error) {
	type initResult struct {
		vendor InitializedVendor
		err    error
	}
	initAsync := func(v Vendor, c chan<- initResult) {
		iv, err := v.Init()
		c <- initResult{iv, err}
	}
	var wg sync.WaitGroup
	wg.Add(len(vendors))

	ch := make(chan initResult)
	go func() {
		wg.Wait()
		close(ch)
	}()

	for _, vendor := range vendors {
		go initAsync(vendor, ch)
	}

	var vendor InitializedVendor
	for res := range ch {
		if res.err == nil {
			vendor = res.vendor
		} else {
			log.Debug(res.err)
		}
		wg.Done()
	}
	if vendor == nil {
		return nil, errors.New("The cloud vendor couldn't be detected")
	}
	return vendor, nil
}
