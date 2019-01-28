// Defines DBIO struct and connection functions

package dbIO

import (
	"database/sql"
	"fmt"
	"github.com/Songmu/prompter"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"time"
)

type DBIO struct {
	DB        *sql.DB
	Host	  string
	Database  string
	User      string
	Password  string
	Starttime time.Time
	Columns   map[string]string
}

func (d *DBIO) create(database string) {
	// Creates new database with utf8 charset
	cmd, err := d.DB.Prepare(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s CHARACTER SET utf8mb4;", database))
	if err != nil {
		fmt.Printf("\t[Error] Formatting command to create database %s: %v\n", database, err)
	} else {
		_, err = cmd.Exec()
		if err != nil {
			fmt.Printf("\t[Error] Creating database %s: %v\n", database, err)
		}
	}
}

func CreateDatabase(host, database, user string) *DBIO {
	// Connects and creates new database
	d := Connect(host, "", user)
	d.create(database)
	// Return conneciton to given database
	d.Database = database
	d.connect()
	return d
}

func ReplaceDatabase(host, database, user string) *DBIO {
	// Deletes database and creates new one (for testing)
	d := Connect(host, "", user)
	cmd, err := d.DB.Prepare(fmt.Sprintf("DROP DATABASE IF EXISTS %s;", database))
	if err != nil {
		fmt.Printf("\t[Error] Formatting command to delete database %s: %v\n", database, err)
	} else {
		_, err = cmd.Exec()
		if err != nil {
			fmt.Printf("\t[Error] Deleting database %s: %v\n", database, err)
		} else {
			d.create(database)
		}
	}
	// Return dcnneciton to given database
	d.Database = database
	d.connect()
	return d
}

func (d *DBIO) connect() {
	// Connects to database
	var err error
	if d.User != "guest" && len(d.Password) < 1 {
		// Prompt for password
		d.Password = prompter.Password("\n\tEnter MySQL password")
	}
	// Begin recording time after password input
	d.Starttime = time.Now()
	cmd := d.User + ":" + d.Password + "@" + d.Host + "/"
	if len(d.Database) > 0 {
		// Connect to specific database
		cmd = cmd + d.Database + "?charset=utf8mb4"
	}
	d.DB, err = sql.Open("mysql", cmd)
	if err != nil {
		fmt.Printf("\n\t[Error] Incorrect username or password: %v", err)
		os.Exit(1000)
	}
	if err = d.DB.Ping(); err != nil {
		fmt.Printf("\n\t[Error] Cannot connect to database: %v", err)
	}
}

func Connect(host, database, user string) *DBIO {
	// Attempts to connect to sql database. Returns dbio instance.
	d := new(DBIO)
	d.Host = host
	d.Database = database
	d.User = user
	d.connect()
	return d
}
