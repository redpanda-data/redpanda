package cloud

import (
	"errors"
	"sync"
	"vectorized/pkg/cloud/vendor"

	log "github.com/sirupsen/logrus"
)

func vendors() map[string]vendor.Vendor {
	vendors := make(map[string]vendor.Vendor)
	aws := &AwsVendor{}
	vendors[aws.Name()] = aws

	return vendors
}

// Tries to initializes the vendors and returns the one available, or an error
// if none could be initialized.
func AvailableVendor() (vendor.InitializedVendor, error) {
	return availableVendorFrom(vendors())
}

func availableVendorFrom(
	vendors map[string]vendor.Vendor,
) (vendor.InitializedVendor, error) {
	type initResult struct {
		vendor vendor.InitializedVendor
		err    error
	}
	initAsync := func(v vendor.Vendor, c chan<- initResult) {
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

	for _, v := range vendors {
		go initAsync(v, ch)
	}

	var v vendor.InitializedVendor
	for res := range ch {
		if res.err == nil {
			v = res.vendor
		} else {
			log.Debug(res.err)
		}
		wg.Done()
	}
	if v == nil {
		return nil, errors.New("The cloud vendor couldn't be detected")
	}
	return v, nil
}
