package utils

import (
	"io/ioutil"
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"
)

func IsAWSi3MetalInstance() bool {
	log.Debug("Checking if we are running on i3.metal amazon instance type")
	timeout := time.Duration(500 * time.Millisecond)
	client := http.Client{
		Timeout: timeout,
	}
	resp, err := client.Get("http://169.254.169.254/latest/meta-data/instance-type")
	if err != nil {
		log.Debug("Can not contact AWS meta-data API, not running in EC2")
		return false
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	instanceType := string(body)
	if err != nil {
		log.Debug("Can not read AWS meta-data API response body")
		return false
	}
	log.Debugf("Running on '%s' EC2 instance", instanceType)

	if instanceType == "i3.metal" {
		return true
	}
	return false
}
