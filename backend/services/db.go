// services/db.go
package services

import (
  "context"
  "log"
  "os"
  "github.com/jackc/pgx/v5"
)

func MustConnectDB() *pgx.Conn {
  url := os.Getenv("DATABASE_URL") // e.g. postgres://user:pass@localhost:5432/ddm_db
  conn, err := pgx.Connect(context.Background(), url)
  if err != nil { log.Fatal(err) }
  return conn
}
