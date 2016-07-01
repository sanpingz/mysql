package mysql

import (
	// "database/sql"
	// "fmt"
	"testing"

	"github.com/sanpingz/sql"
)

func TestMySQL(t *testing.T) {
	if db, err := sql.Open(MySQLDriverName, ""); err != nil {
		t.Error(err)
	} else if err := db.Ping(); err != nil {
		t.Error(err)
	}
}
