package storage

import (
	"database/sql"
	"fmt"

	_ "github.com/GoogleCloudPlatform/cloudsql-proxy/proxy/dialers/postgres"
	"github.com/sirupsen/logrus"
)

type PostgresRepo struct {
	Db *sql.DB
}

func NewPostgresRepo(
	dbName, instanceConnName, user, pass string,
) (*PostgresRepo, error) {
	params := fmt.Sprintf(
		"host=%s user=%s password=%s dbname=%s sslmode=disable",
		instanceConnName,
		user,
		pass,
		dbName,
	)
	db, err := sql.Open("cloudsqlpostgres", params)
	if err != nil {
		return nil, err
	}
	return &PostgresRepo{db}, nil
}

func (r *PostgresRepo) SaveMetrics(m Metrics) error {
	query := "INSERT INTO metrics(sent_at," +
		" received_at," +
		" organization," +
		" cluster_id," +
		" node_id," +
		" node_uuid," +
		" free_memory_mb," +
		" free_space_mb," +
		" cpu_percentage)" +
		" VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9);"
	stmt, err := r.Db.Prepare(query)
	if err != nil {
		return err
	}
	_, err = stmt.Exec(
		m.SentAt,
		m.ReceivedAt,
		m.Organization,
		m.ClusterId,
		m.NodeId,
		m.NodeUuid,
		m.FreeMemoryMB,
		m.FreeSpaceMB,
		m.CpuPercentage,
	)
	if err != nil {
		return err
	}
	return nil
}

func (r *PostgresRepo) SaveEnvironment(e Environment) error {
	logrus.Infof("Payload: %s", e.Payload)
	logrus.Infof("Config: %s", e.Config)
	query := "INSERT INTO environment(" +
		" sent_at," +
		" received_at," +
		" organization," +
		" cluster_id," +
		" node_id," +
		" node_uuid," +
		" payload," +
		" config)" +
		" VALUES($1, $2, $3, $4, $5, $6, $7, $8);"
	stmt, err := r.Db.Prepare(query)
	if err != nil {
		return err
	}
	_, err = stmt.Exec(
		e.SentAt,
		e.ReceivedAt,
		e.Organization,
		e.ClusterId,
		e.NodeId,
		e.NodeUuid,
		e.Payload,
		e.Config,
	)
	return err
}
