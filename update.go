// This script contains general functions for extracting data from a database

package dbIO

import (
	"fmt"
	"strings"
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

func wrapApo(v string) string {
	// Wraps calls escapeChars and v in appstrophes
	if strings.Count(v, " ") > 0 {
		return fmt.Sprintf("'%s'", escapeChars(v))
	}
	return v
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

func (d *DBIO) UpdateRows(table, idcol, column string, values map[string]map[string]string) bool {
	// Updates column where id column = key with value
	var cmd strings.Builder
	cmd.WriteString(fmt.Sprintf("UPDATE %s CASE %s", table, idcol))
	for key, value := range values {
		for k, v := range value {
			cmd.WriteString(fmt.Sprintf(" WHEN '%s' THEN SET '%s' = '%s';", key, k, v))
		}
	}
	return d.update(table, cmd.String())
}

func (d *DBIO) UpdateRow(table, target, value, column, op, key string) bool {
	// Updates single column in table, returns true if successful
	return d.update(table, fmt.Sprintf("UPDATE %s SET %s = %s WHERE %s %s %s;", table, target, wrapApo(value), column, op, wrapApo(key)))
}

func (d *DBIO) DeleteRow(table, column, value string) {
	// Deletes row(s) from database where column name = given value
	cmd, err := d.DB.Prepare(fmt.Sprintf("DELETE FROM %s WHERE %s = '%s';", table, column, value))
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
