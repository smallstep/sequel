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

var postgresDataSource string

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
	var cleanups []func()
	cleanup := func(fn func()) {
		cleanups = append(cleanups, fn)
	}
	fatal := func(args ...any) {
		fmt.Fprintln(os.Stderr, args...)
		for _, fn := range cleanups {
			fn()
		}
		os.Exit(1)
	}

	ctx := context.Background()
	postgresContainer, err := postgres.Run(ctx, postgresImage,
		postgres.WithDatabase(dbName),
		postgres.WithUsername(dbUser),
		postgres.WithPassword(dbPassword),
		postgres.WithInitScripts(filepath.Join("testdata", "init-db.sh")),
		withSchemaSQL(),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second),
		),
	)
	if err != nil {
		fatal("error creating postgres container:", err)
	}
	cleanup(func() {
		if err := postgresContainer.Terminate(ctx); err != nil {
			fmt.Fprintln(os.Stderr, "error terminating postgres:", err)
		}
	})

	postgresState, err := postgresContainer.State(ctx)
	if err != nil {
		fatal(err)
	}
	if !postgresState.Running {
		fatal("Postgres status:", postgresState.Status)
	}

	postgresPort, err := postgresContainer.MappedPort(ctx, "5432/tcp")
	if err != nil {
		fatal(err)
	}

	postgresDataSource = fmt.Sprintf("postgres://%s:%s@localhost:%s/%s?sslmode=disable&application_name=test", dbUser, dbPassword, postgresPort.Port(), dbName)

	os.Exit(m.Run())
}
