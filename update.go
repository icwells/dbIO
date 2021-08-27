// This script contains general functions for extracting data from a database

package dbIO

import (
	"fmt"
	"strings"
	"time"
)

// OptimizeTables calls optimize on all tables in the database.
func (d *DBIO) OptimizeTables() {
	for k := range d.Columns {
		cmd, err := d.DB.Prepare(fmt.Sprintf("OPTIMIZE TABLE %s;", k))
		if err != nil {
			d.logger.Printf("[Error] Formatting command to optimize table %s: %v\n", k, err)
		} else {
			cmd.Exec()
		}
	}
}

// TruncateTable clears all content from the given table.
func (d *DBIO) TruncateTable(table string) {
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

// GetUpdateTimes returns a map the last update date and time for each table.
func (d *DBIO) GetUpdateTimes() map[string]time.Time {
	ret := make(map[string]time.Time)
	for k := range d.Columns {
		cmd := fmt.Sprintf("SELECT UPDATE_TIME FROM information_schema.tables WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s';", d.Database, k)
		rows := d.Execute(cmd)
		if len(rows) > 0 && !strings.Contains(rows[0][0], "<nil>") {
			t, err := time.Parse("2006-01-02 15:04:05", rows[0][0])
			if err == nil {
				ret[k] = t
			} else {
				d.logger.Printf("[Error] Converting timestamp %s: %v\n", rows[0][0], err)
			}
		}
	}
	return ret
}

// LastUpdate returns the time of the most recent update.
func (d *DBIO) LastUpdate() time.Time {
	var ret time.Time
	t := d.GetUpdateTimes()
	for _, v := range t {
		if v.After(ret) {
			ret = v
		}
	}
	return ret
}

// Submits update command and returns true if successful
func (d *DBIO) update(table, command string) bool {
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

// UpdateColumns updates columns (specified as outer map key) in table where column == inner map key with map values. Returns true if successful.
func (d *DBIO) UpdateColumns(table, idcol string, values map[string]map[string]string) bool {
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

// UpdateRow updates a single column in the given table and returns true if successful.
func (d *DBIO) UpdateRow(table, target, value, column, op, key string) bool {
	return d.update(table, fmt.Sprintf("UPDATE %s SET %s = '%s' WHERE %s %s '%s';", table, target, value, column, op, key))
}

// Performs given deletion command
func (d *DBIO) deleteEntries(table, command string) {
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

// DeleteRows deletes rows from the database if the value in the given column is contained in the values slice.
func (d *DBIO) DeleteRows(table, column string, values []string) {
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

// DeleteRow deletes a single row from the database where the value in the given column equals value.
func (d *DBIO) DeleteRow(table, column, value string) {
	d.deleteEntries(table, fmt.Sprintf("DELETE FROM %s WHERE %s = '%s';", table, column, value))
}
