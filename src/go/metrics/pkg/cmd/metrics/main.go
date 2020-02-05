package main

import (
	"metrics/pkg/http"
	"metrics/pkg/storage"
	"os"
	"strconv"
)

func main() {
	var err error
	port := 8080
	envPort := os.Getenv("PORT")
	dbName := os.Getenv("DB_NAME")
	dbInstanceConnName := os.Getenv("DB_INSTANCE_CONN_NAME")
	dbUser := os.Getenv("DB_USER")
	dbPass := os.Getenv("DB_PASS")
	dbMigrationsUrl := os.Getenv("DB_MIGRATIONS_URL")
	if envPort != "" {
		port, err = strconv.Atoi(envPort)
		if err != nil {
			panic(err)
		}
	}
	repo, err := storage.NewPostgresRepo(dbName, dbInstanceConnName, dbUser, dbPass)
	if err != nil {
		panic(err)
	}
	err = storage.Migrate(repo.Db, dbName, dbMigrationsUrl)
	if err != nil {
		panic(err)
	}
	metricsHandler := &http.MetricsHandler{repo}
	envHandler := &http.EnvHandler{repo}
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
