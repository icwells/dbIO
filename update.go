// This script contains general functions for extracting data from a database

package dbIO

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/Songmu/prompter"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"strings"
	"time"
)

type DBIO struct {
	DB        *sql.DB
	Database  string
	User      string
	Password  string
	Starttime time.Time
	Columns   map[string]string
}

func Connect(database, user string) *DBIO {
	// Attempts to connect to sql database. Returns dbio instance.
	var err error
	d := new(DBIO)
	d.Database = database
	d.User = user
	if d.User != "guest" {
		// Prompt for password
		d.Password = prompter.Password("\n\tEnter MySQL password")
	}
	// Begin recording time after password input
	d.Starttime = time.Now()
	d.DB, err = sql.Open("mysql", d.User+":"+d.Password+"@/"+d.Database)
	if err != nil {
		fmt.Printf("\n\t[Error] Incorrect username or password: %v", err)
		os.Exit(1000)
	}
	if err = d.DB.Ping(); err != nil {
		fmt.Printf("\n\t[Error] Cannot connect to database: %v", err)
	}
	return d
}

func (d *DBIO) TruncateTable(table string) {
	// Clears all table contents
	cmd, err := d.DB.Prepare(fmt.Sprintf("TRUNCATE TABLE %s;", table))
	if err != nil {
		fmt.Printf("\t[Error] Formatting command to truncate table %s: %v\n", table, err)
	} else {
		_, err = cmd.Exec()
		if err != nil {
			fmt.Printf("\t[Error] Truncating table %s: %v\n", table, err)
		}
	}
}

func columnEqualTo(columns string, values [][]string) []string {
	// Matches columns to inner slice by index, returns empty slice if indeces are not equal
	var ret []string
	col := strings.Split(columns, ",")
	for _, val := range values {
		if len(val) == len(col) {
			// Concatenate string for each row
			first := true
			buffer := bytes.NewBufferString("")
			for idx, i := range val {
				if first == false {
					// Write seperating comma
					buffer.WriteByte(',')
				}
				if len(i) >= 1 {
					// Leave empty fields unchanged
					buffer.WriteString(col[idx])
					buffer.WriteByte('=')
					buffer.WriteByte('\'')
					buffer.WriteString(i)
					buffer.WriteByte('\'')
					first = false
				}
			}
			ret = append(ret, buffer.String())
		}
	}
	return ret
}

func (d *DBIO) UpdateRow(table, target, key string, values [][]string) int {
	// Updates rows where target = key with values (matched to columns)
	ret := 0
	val := columnEqualTo(d.Columns[table], values)
	for _, i := range val {
		cmd, err := d.DB.Prepare(fmt.Sprintf("UPDATE %s SET %s WHERE %s = %s;", table, i, target, key))
		if err != nil {
			fmt.Printf("\t[Error] Preparing update for %s: %v\n", table, err)
		} else {
			_, err = cmd.Exec()
			cmd.Close()
			if err != nil {
				fmt.Printf("\t[Error] Updating row(s) from %s: %v\n", table, err)
			} else {
				ret++
			}
		}
	}
	return ret
}

func (d *DBIO) DeleteRow(table, column, value string) {
	// Deletes row(s) from database where column name = given value
	cmd, err := d.DB.Prepare(fmt.Sprintf("DELETE FROM %s WHERE %s = '%s';", table, column, value))
	if err != nil {
		fmt.Printf("\t[Error] Preparing deletion from %s: %v\n", table, err)
	} else {
		_, err = cmd.Exec()
		cmd.Close()
		if err != nil {
			fmt.Printf("\t[Error] Deleting row(s) from %s: %v\n", table, err)
		}
	}
}
