// Defines DBIO struct and connection functions

package dbIO

import (
	"bufio"
	"database/sql"
	"fmt"
	"github.com/Songmu/prompter"
	// MySQL driver is required in sql package
	_ "github.com/go-sql-driver/mysql"
	"log"
	"os"
	"strings"
	"time"
)

// DBIO is the central struct containing all releveant connection information.
type DBIO struct {
	// DB is the database connection. Any SQL query can be run directly using DB.
	DB *sql.DB
	// Host is the host IP.
	Host string
	// Database stores the name of the database.
	Database string
	// User is the MySQL user name used in this session.
	User string
	// Password stores the user's password. It is not secure, so be careful how you use it.
	Password string
	// Starttime is the time point after the password is given.
	Starttime time.Time
	// Columns stores a map with a comma-seperated string of column name for each table.
	Columns map[string]string
	logger  *log.Logger
}

// NewDBIO returns an initialized struct. If host is left blank, it will default to localHost.
func NewDBIO(host, database, user, password string) *DBIO {
	d := new(DBIO)
	host = strings.TrimSpace(host)
	if len(host) < 1 {
		d.Host = "localhost"
	} else {
		d.Host = fmt.Sprintf("tcp(%s:3306)", host)
	}
	d.Database = database
	d.User = user
	d.Password = password
	d.logger = log.New(os.Stderr, "dbIO_Log: ", log.Ldate|log.Ltime)
	return d
}

// Creates new database with utf8 charset
func (d *DBIO) create(database string) {
	cmd, err := d.DB.Prepare(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s CHARACTER SET utf8mb4;", database))
	if err != nil {
		d.logger.Printf("[Error] Formatting command to create database %s: %v\n", database, err)
	} else {
		_, err = cmd.Exec()
		if err != nil {
			d.logger.Printf("[Error] Creating database %s: %v\n", database, err)
		}
	}
}

// CreateDatabase connects to MySQL and creates a new database.
func CreateDatabase(host, database, user string) *DBIO {
	d, err := Connect(host, "", user, "")
	if err != nil {
		d.logger.Fatalln(err)
	}
	d.create(database)
	// Return conneciton to given database
	d.Database = database
	d.connect()
	return d
}

// ReplaceDatabase deletes the given database and creates a new, empty, one (for testing).
func ReplaceDatabase(host, database, user, password string) *DBIO {
	d, err := Connect(host, "", user, password)
	if err != nil {
		d.logger.Fatalln(err)
	}
	cmd, err := d.DB.Prepare(fmt.Sprintf("DROP DATABASE IF EXISTS %s;", database))
	if err != nil {
		d.logger.Printf("[Error] Formatting command to delete database %s: %v\n", database, err)
	} else {
		_, err = cmd.Exec()
		if err != nil {
			d.logger.Printf("[Error] Deleting database %s: %v\n", database, err)
		} else {
			d.create(database)
		}
	}
	// Return conneciton to given database
	d.Database = database
	d.connect()
	return d
}

// Connects to database
func (d *DBIO) connect() error {
	var err error
	if d.User == "" {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("\n\tEnter MySQL user name: ")
		text, _ := reader.ReadString('\n')
		d.User = strings.TrimSpace(text)
	}
	if d.User != "guest" && d.Password == "" {
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
	return err
}

// Connect attempts to connect to the MySQL database located at host/database using the given user name and password.
func Connect(host, database, user, password string) (*DBIO, error) {
	d := NewDBIO(host, database, user, password)
	err := d.connect()
	if err != nil {
		err = fmt.Errorf("\n\t[Error] Incorrect username or password: %v", err)
	}
	if err = d.DB.Ping(); err != nil {
		err = fmt.Errorf("\n\t[Error] Cannot connect to database: %v", err)
	}
	return d, err
}

// Ping returns true if the given credentials are valid, and discards the connection.
func Ping(host, database, user, password string) bool {
	ret := false
	d := NewDBIO(host, database, user, password)
	err := d.connect()
	if err == nil {
		if err = d.DB.Ping(); err == nil {
			// Return true if no errors are encountered
			ret = true
		}
	}
	return ret
}
