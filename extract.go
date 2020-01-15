// This script contains general functions for extracting data from a database

package dbIO

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"
)

// Returns integer from query
func (d *DBIO) getCount(table, cmd string) int {
	var n int
	val := d.DB.QueryRow(cmd)
	err := val.Scan(&n)
	if err != nil {
		d.logger.Printf("[Error] Counting entries from %s: %v\n\n", table, err)
	}
	return n
}

// Count returns count of entries from target column(s) in table where key relates to column via op (>=/=/...; ie. column >= 7).
// Returns total if distinct is false; returns number of unique entries if distinct is true.
// Give operator, key, and target as emtpy strings to count without evaluating.
func (d *DBIO) Count(table, column, target, op, key string, distinct bool) int {
	var cmd string
	if distinct == true {
		cmd = fmt.Sprintf("SELECT COUNT(DISTINCT %s) FROM %s", target, table)
	} else {
		cmd = fmt.Sprintf("SELECT COUNT(%s) FROM %s", target, table)
	}
	if len(op) >= 1 || len(key) >= 1 || len(column) >= 1 {
		if len(op) >= 1 && len(key) >= 1 && len(column) >= 1 {
			// Add evaluation statement
			cmd += fmt.Sprintf(" WHERE %s %s '%s'", column, op, key)
		} else {
			fmt.Print("\n\t[Error] Please specify target column, operator, and target value. Returning -1.\n")
			return -1
		}
	}
	return d.getCount(table, cmd)
}

// CountRows returns the number of rows from the given table.
func (d *DBIO) CountRows(table string) int {
	cmd := fmt.Sprintf("SELECT COUNT(*) FROM %s;", table)
	return d.getCount(table, cmd)
}

// GetMax returns the highest number from the given column.
func (d *DBIO) GetMax(table, column string) int {
	var m int
	n := d.CountRows(table)
	if n > 0 {
		cmd := fmt.Sprintf("SELECT MAX(%s) FROM %s;", column, table)
		val := d.DB.QueryRow(cmd)
		err := val.Scan(&m)
		if err != nil {
			d.logger.Printf("[Error] Determining maximum value from %s in %s: %v\n\n", column, table, err)
		}
	} else {
		m = n
	}
	return m
}

// Returns rows of uncertain length as slice of string slices
func toSlice(rows *sql.Rows) [][]string {
	var ret [][]string
	columns, _ := rows.Columns()
	count := len(columns)
	values := make([]interface{}, count)
	pointers := make([]interface{}, count)
	for rows.Next() {
		var r []string
		for i := range columns {
			pointers[i] = &values[i]
		}
		// Maps items to values via pointers
		rows.Scan(pointers...)
		for _, i := range values {
			// Use Sprintf to convert interface to string
			val := fmt.Sprintf("%s", i)
			r = append(r, val)
		}
		ret = append(ret, r)
	}
	return ret
}

// Execute submits the given command as a MySQL query.
func (d *DBIO) Execute(cmd string) [][]string {
	rows, err := d.DB.Query(cmd)
	if err != nil {
		d.logger.Printf("[Error] Executing '%s': %v", cmd, err)
	}
	defer rows.Close()
	return toSlice(rows)
}

// GetRowsMin returns all rows of target columns where column >= key.
func (d *DBIO) GetRowsMin(table, column, target string, min int) [][]string {
	var cmd string
	cmd = fmt.Sprintf("SELECT %s FROM %s WHERE %s >= %d;", target, table, column, min)
	return d.Execute(cmd)
}

// Wraps terms in apostrophes to avoid errors
func addApostrophes(key string) string {
	s := strings.Split(key, ",")
	buffer := bytes.NewBufferString("")
	for idx, i := range s {
		if idx != 0 {
			// Write preceding comma
			buffer.WriteByte(',')
		}
		buffer.WriteByte('\'')
		buffer.WriteString(i)
		buffer.WriteByte('\'')
	}
	return buffer.String()
}

// GetRows returns rows of target columns with key in column.
func (d *DBIO) GetRows(table, column, key, target string) [][]string {
	var cmd string
	if strings.Contains(key, ",") == true {
		// Format for list
		if strings.Contains(key, "'") == false {
			key = addApostrophes(key)
		}
		cmd = fmt.Sprintf("SELECT %s FROM %s WHERE %s IN (%s);", target, table, column, key)
	} else {
		cmd = fmt.Sprintf("SELECT %s FROM %s WHERE %s = '%s';", target, table, column, key)
	}
	return d.Execute(cmd)
}

// EvaluateRows returns rows of columns where key relates to target via op (>=/=/...) (i.e. column <= key).
func (d *DBIO) EvaluateRows(table, column, op, key, target string) [][]string {
	cmd := fmt.Sprintf("SELECT %s FROM %s WHERE %s %s '%s';", target, table, column, op, key)
	return d.Execute(cmd)
}

// GetColumnInt returns a slice of all entries in column of integers.
func (d *DBIO) GetColumnInt(table, column string) []int {
	var col []int
	sql := fmt.Sprintf("SELECT %s FROM %s;", column, table)
	rows, err := d.DB.Query(sql)
	if err != nil {
		d.logger.Printf("[Error] Extracting %s column from %s: %v", column, table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var val int
		// Assign data to val while checking err
		if err := rows.Scan(&val); err != nil {
			d.logger.Printf("[Error] Reading %s from %s: %v", column, table, err)
		}
		col = append(col, val)
	}
	return col
}

// GetColumnText returns a slice of all entries in column of text.
func (d *DBIO) GetColumnText(table, column string) []string {
	var col []string
	sql := fmt.Sprintf("SELECT %s FROM %s;", column, table)
	rows, err := d.DB.Query(sql)
	if err != nil {
		d.logger.Printf("[Error] Extracting %s column from %s: %v", column, table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var val string
		// Assign data to val while checking err
		if err := rows.Scan(&val); err != nil {
			d.logger.Printf("[Error] Reading %s from %s: %v", column, table, err)
		}
		col = append(col, val)
	}
	return col
}

// GetColumns returns a slice of slices of all entries in given columns.
func (d *DBIO) GetColumns(table string, columns []string) [][]string {
	cmd := fmt.Sprintf("SELECT %s FROM %s;", strings.Join(columns, ","), table)
	return d.Execute(cmd)
}

// GetNumOccurances returns a map with the number of unique entries in column.
func (d *DBIO) GetNumOccurances(table, column string) map[string]int {
	occ := make(map[string]int)
	entries := d.GetColumnText(table, column)
	for _, i := range entries {
		if _, ex := occ[i]; ex == true {
			occ[i]++
		} else {
			occ[i] = 1
		}
	}
	return occ
}

// GetTable returns all contents of the given table.
func (d *DBIO) GetTable(table string) [][]string {
	cmd := fmt.Sprintf("SELECT * FROM %s ;", table)
	return d.Execute(cmd)
}

// GetTableMap returns the given table as a map with the first column as the key.
func (d *DBIO) GetTableMap(table string) map[string][]string {
	tbl := make(map[string][]string)
	s := d.GetTable(table)
	for _, i := range s {
		tbl[i[0]] = i[1:]
	}
	return tbl
}
