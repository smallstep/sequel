package sequel

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	dbName        = "sequel"
	dbUser        = "test"
	dbPassword    = "password"
	postgresImage = "docker.io/postgres:16.0-alpine"
)

// Connection strings for the three databases created in [TestMain]
// These are used in other tests
var (
	postgresDataSource    string
	postgresDataSourceRR1 string
	postgresDataSourceRR2 string
)

func withSchemaSQL() testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		req.Files = append(req.Files, testcontainers.ContainerFile{
			HostFilePath:      filepath.Join("testdata", "schema.sql"),
			ContainerFilePath: "/tmp/schema.sql",
			FileMode:          0644,
		})
		return nil
	}
}

func TestMain(m *testing.M) {
	ctx := context.Background()
	cleanups, err := createPostgresContainers(ctx)
	defer func() {
		for _, cleanup := range cleanups {
			if cleanup != nil {
				cleanup()
			}
		}
	}()
	if err != nil {
		fmt.Printf("did not create postgres containers: %v\n", err)
	}

	// nolint:gocritic // The docs for Run specify that the returned int is to be passed to os.Exit
	os.Exit(m.Run())
}

// createPostgresContainers creates 3 databases as containers. The first is intended to mimic a master database and
// the last 2 are intended to mimic read replicas.
// A []func() is returned to cleanups.
// Package-level connction strings are set.
func createPostgresContainers(ctx context.Context) ([]func(), error) {
	// Database connection strings are the same, except the port
	connString := func(port string) string {
		return fmt.Sprintf("postgres://%s:%s@localhost:%s/%s?sslmode=disable&application_name=test", dbUser, dbPassword, port, dbName)
	}

	var cleanups []func()

	// Create the master database
	cM, mpM, err := createPostgresContainer(ctx, "init-db.sh")
	cleanups = append(cleanups, cM)
	if err != nil {
		return cleanups, err
	}
	postgresDataSource = connString(mpM)

	// Create first read replica
	cRR1, mpRR1, err := createPostgresContainer(ctx, "init-db-rr.sh")
	cleanups = append(cleanups, cRR1)
	if err != nil {
		return cleanups, err
	}
	postgresDataSourceRR1 = connString(mpRR1)

	// Create second read replica
	cRR2, mpRR2, err := createPostgresContainer(ctx, "init-db-rr.sh")
	cleanups = append(cleanups, cRR2)
	if err != nil {
		return cleanups, err
	}
	postgresDataSourceRR2 = connString(mpRR2)

	return cleanups, nil
}

// createPostgresContainer creates a single postgres container on the specified port
func createPostgresContainer(ctx context.Context, initFilename string) (func(), string, error) {
	postgresContainer, err := postgres.Run(ctx, postgresImage,
		postgres.WithDatabase(dbName),
		postgres.WithUsername(dbUser),
		postgres.WithPassword(dbPassword),
		postgres.WithInitScripts(filepath.Join("testdata", initFilename)),
		withSchemaSQL(),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second),
		),
	)
	if err != nil {
		return nil, "", fmt.Errorf("error creating postgres container: %w", err)
	}

	cleanup := func() {
		if err := postgresContainer.Terminate(ctx); err != nil {
			fmt.Fprintln(os.Stderr, "error terminating postgres:", err)
		}
	}

	postgresState, err := postgresContainer.State(ctx)
	if err != nil {
		return cleanup, "", fmt.Errorf("checking container state: %w", err)
	}
	if !postgresState.Running {
		return cleanup, "", fmt.Errorf("Postgres status %q is not \"running\"", postgresState.Status)
	}

	mp, err := postgresContainer.MappedPort(ctx, "5432/tcp")
	if err != nil {
		return cleanup, "", fmt.Errorf("mapped port 5432/tcp does not seem to be available: %w", err)
	}

	return cleanup, mp.Port(), nil
}
