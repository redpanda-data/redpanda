package main

import (
	"metrics/pkg/http"
	"os"
	"strconv"
)

func main() {
	var err error
	port := 8080
	envPort := os.Getenv("PORT")
	if envPort != "" {
		port, err = strconv.Atoi(envPort)
		if err != nil {
			panic(err)
		}
	}
	metricsHandler := &http.MetricsHandler{}
	envHandler := &http.EnvHandler{}
	server := &http.Server{
		Port:    uint(port),
		Metrics: metricsHandler,
		Env:     envHandler,
	}
	err = http.Serve(server)
	if err != nil {
		panic(err)
	}
}
