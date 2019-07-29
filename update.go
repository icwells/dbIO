// This script contains general functions for extracting data from a database

package dbIO

import (
	"bytes"
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

func columnEqualTo(columns string, values []string) string {
	// Matches columns to slice by index, returns empty slice if indeces are not equal
	first := true
	col := strings.Split(columns, ",")
	buffer := bytes.NewBufferString("")
	if len(values) == len(col) {
		for idx, i := range values {
			if len(i) >= 1 {
				// Concatenate string
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
		}
	}
	return buffer.String()
}

func wrapApo(v string) string {
	// Wraps calls escapeChars and v in appstrophes
	if strings.Count(v, " ") > 0 {
		return fmt.Sprintf("'%s'", escapeChars(v))
	}
	return v
}

func (d *DBIO) UpdateRows(table, target string, values map[string][]string) int {
	// Updates rows where target = key with values (matched to columns)
	ret := 0
	for k, v := range values {
		val := columnEqualTo(d.Columns[table], v)
		cmd, err := d.DB.Prepare(fmt.Sprintf("UPDATE %s SET %s WHERE %s = %s;", table, wrapApo(val), target, wrapApo(k)))
		if err != nil {
			d.logger.Printf("[Error] Preparing update for %s: %v\n", table, err)
		} else {
			_, err = cmd.Exec()
			cmd.Close()
			if err != nil {
				d.logger.Printf("[Error] Updating row(s) from %s: %v\n", table, err)
			} else {
				ret++
			}
		}
	}
	return ret
}

func (d *DBIO) UpdateRow(table, target, value, column, op, key string) bool {
	// Updates single column in table, returns true if successful
	ret := true
	cmd, err := d.DB.Prepare(fmt.Sprintf("UPDATE %s SET %s = %s WHERE %s %s %s;", table, target, wrapApo(value), column, op, wrapApo(key)))
	if err != nil {
		d.logger.Printf("[Error] Preparing update for %s: %v\n", table, err)
		ret = false
	} else {
		_, err = cmd.Exec()
		cmd.Close()
		if err != nil {
			d.logger.Printf("[Error] Updating row from %s: %v\n", table, err)
			ret = false
		}
	}
	return ret
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
