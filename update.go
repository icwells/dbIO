// This script contains general functions for extracting data from a database

package dbIO

import (
	"fmt"
	"strings"
	"time"
)

func (d *DBIO) TruncateTable(table string) {
	// Clears all table contents
	cmd, err := d.DB.Prepare(fmt.Sprintf("TRUNCATE TABLE %s;", table))
	if err != nil {
		d.logger.Printf("[Error] Formatting command to truncate table %s: %v\n", table, err)
	} else {
		_, err = cmd.Exec()
		if err != nil {
			d.logger.Printf("[Error] Truncating table %s: %v\n", table, err)
		}
	}
}

func (d *DBIO) GetUpdateTimes() map[string]time.Time {
	// Returns map of table names and the date and time of last update
	ret := make(map[string]time.Time)
	for k := range d.Columns {
		cmd := fmt.Sprintf("SELECT UPDATE_TIME FROM information_schema.tables WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s';", d.Database, k)
		rows := d.Execute(cmd)
		t, err := time.Parse("2019-10-15 14:42:36", rows[0][0])
		if err == nil {
			ret[k] = t
		} else {
			d.logger.Printf("[Error] Converting timestamp %s: %v\n", rows[0][0], err)
		}
	}
	return ret
}

func (d *DBIO) LastUpdate() time.Time {
	// Returns time of latest update
	var ret time.Time
	t := d.GetUpdateTimes()
	for _, v := range t {
		if ret.IsZero() || v.After(ret) {
			ret = v
		}
	}
	return ret
}

func (d *DBIO) update(table, command string) bool {
	// Submits update command and returns true if successful
	ret := true
	cmd, err := d.DB.Prepare(command)
	if err != nil {
		d.logger.Printf("[Error] Preparing update for %s: %v\n", table, err)
		ret = false
	} else {
		_, err = cmd.Exec()
		cmd.Close()
		if err != nil {
			d.logger.Printf("[Error] Updating row(s) from %s: %v\n", table, err)
			ret = false
		}
	}
	return ret
}

func (d *DBIO) UpdateColumns(table, idcol string, values map[string]map[string]string) bool {
	// Updates column where id column = key with value
	var cmd strings.Builder
	first := true
	cmd.WriteString(fmt.Sprintf("UPDATE %s SET", table))
	for key, value := range values {
		if first == false {
			// Seperate columns by comma
			cmd.WriteByte(',')
		}
		cmd.WriteString(fmt.Sprintf("\n%s = CASE\n", key))
		for k, v := range value {
			cmd.WriteString(fmt.Sprintf("\tWHEN %s='%s' THEN '%s'\n", idcol, k, v))
		}
		cmd.WriteString(fmt.Sprintf("ELSE %s END", key))
		first = false
	}
	cmd.WriteString(fmt.Sprintf("\nWHERE %s IS NOT NULL;", idcol))
	return d.update(table, cmd.String())
}

func (d *DBIO) UpdateRow(table, target, value, column, op, key string) bool {
	// Updates single column in table, returns true if successful
	return d.update(table, fmt.Sprintf("UPDATE %s SET %s = '%s' WHERE %s %s '%s';", table, target, value, column, op, key))
}

func (d *DBIO) deleteEntries(table, command string) {
	// Performs given deletion command
	cmd, err := d.DB.Prepare(command)
	if err != nil {
		d.logger.Printf("[Error] Preparing deletion from %s: %v\n", table, err)
	} else {
		_, err = cmd.Exec()
		cmd.Close()
		if err != nil {
			d.logger.Printf("[Error] Deleting row(s) from %s: %v\n", table, err)
		}
	}
}

func (d *DBIO) DeleteRows(table, column string, values []string) {
	// Deletes rows from database where column value in values
	var b strings.Builder
	first := true
	for _, i := range values {
		if first == false {
			b.WriteByte(',')
		}
		b.WriteByte('\'')
		b.WriteString(i)
		b.WriteByte('\'')
		first = false
	}
	d.deleteEntries(table, fmt.Sprintf("DELETE FROM %s WHERE %s IN (%s);", table, column, b.String()))
}

func (d *DBIO) DeleteRow(table, column, value string) {
	// Deletes row from database where column name = given value
	d.deleteEntries(table, fmt.Sprintf("DELETE FROM %s WHERE %s = '%s';", table, column, value))
}
