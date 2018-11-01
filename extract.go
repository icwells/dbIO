// This script contains general functions for extracting data from a database

package dbIO

import (
	"bytes"
	"database/sql"
	"fmt"
	"github.com/icwells/go-tools/strarray"
	"strings"
)

func GetCount(db *sql.DB, table string) int {
	// Returns number of rows from table
	var n int
	cmd := fmt.Sprintf("SELECT COUNT(*) FROM %s;", table)
	val := db.QueryRow(cmd)
	err := val.Scan(&n)
	if err != nil {
		fmt.Printf("\n\t[Error] Determining number of rows from %s: %v\n\n", table, err)
	}
	return n
}

func GetMax(db *sql.DB, table, column string) int {
	// Returns maximum number from given column
	var m int
	n := GetCount(db, table)
	if n > 0 {
		cmd := fmt.Sprintf("SELECT MAX(%s) FROM %s;", column, table)
		val := db.QueryRow(cmd)
		err := val.Scan(&m)
		if err != nil {
			fmt.Printf("\n\t[Error] Determining maximum value from %s in %s: %v\n\n", column, table, err)
		}
	} else {
		m = n
	}
	return m
}

func toSlice(rows *sql.Rows) [][]string {
	// Returns rows of uncertain length as slice of string slices
	var ret [][]string
	columns, _ := rows.Columns()
	count := len(columns)
	values := make([]interface{}, count)
	pointers := make([]interface{}, count)
	for rows.Next() {
		var r []string
		for i, _ := range columns {
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

func addApprostrophes(key string) string {
	// Wraps terms in apostrophes to avoid errors 
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

func GetRowsMin(db *sql.DB, table, column, target string, min int) [][]string {
	// Returns rows of target columns with column >= key
	var cmd string
	cmd = fmt.Sprintf("SELECT %s FROM %s WHERE %s >= %d;", target, table, column, min)
	rows, err := db.Query(cmd)
	if err != nil {
		fmt.Printf("\n\t[Error] Extracting rows from %s: %v", table, err)
	}
	defer rows.Close()
	return toSlice(rows)
}

func GetRows(db *sql.DB, table, column, key, target string) [][]string {
	// Returns rows of target columns with key in column
	var cmd string
	if strings.Contains(key, ",") == true {
		// Format for list
		if strings.Contains(key, "'") == false {
			key = addApprostrophes(key)
		}
		cmd = fmt.Sprintf("SELECT %s FROM %s WHERE %s IN (%s);", target, table, column, key)
	} else {
		cmd = fmt.Sprintf("SELECT %s FROM %s WHERE %s = '%s';", target, table, column, key)
	}
	rows, err := db.Query(cmd)
	if err != nil {
		fmt.Printf("\n\t[Error] Extracting rows from %s: %v", table, err)
	}
	defer rows.Close()
	return toSlice(rows)
}

func EvaluateRows(db *sql.DB, table, column, op, key, target string) [][]string {
	// Returns rows of target columns where key relates to column via op (>=/=/...)
	cmd := fmt.Sprintf("SELECT %s FROM %s WHERE %s %s '%s';", target, table, column, op, key)
	rows, err := db.Query(cmd)
	if err != nil {
		fmt.Printf("\n\t[Error] Extracting rows from %s: %v", table, err)
	}
	defer rows.Close()
	return toSlice(rows)
}

func GetColumnInt(db *sql.DB, table, column string) []int {
	// Returns slice of all entries in column of integers
	var col []int
	sql := fmt.Sprintf("SELECT %s FROM %s;", column, table)
	rows, err := db.Query(sql)
	if err != nil {
		fmt.Printf("\n\t[Error] Extracting %s column from %s: %v", column, table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var val int
		// Assign data to val while checking err
		if err := rows.Scan(&val); err != nil {
			fmt.Printf("\n\t[Error] Reading %s from %s: %v", column, table, err)
		}
		col = append(col, val)
	}
	return col
}

func GetColumnText(db *sql.DB, table, column string) []string {
	// Returns slice of all entries in column of text
	var col []string
	sql := fmt.Sprintf("SELECT %s FROM %s;", column, table)
	rows, err := db.Query(sql)
	if err != nil {
		fmt.Printf("\n\t[Error] Extracting %s column from %s: %v", column, table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var val string
		// Assign data to val while checking err
		if err := rows.Scan(&val); err != nil {
			fmt.Printf("\n\t[Error] Reading %s from %s: %v", column, table, err)
		}
		col = append(col, val)
	}
	return col
}

func GetColumns(db *sql.DB, table string, columns []string) [][]string {
	// Returns slice of slices of all entries in given columns of text
	sql := fmt.Sprintf("SELECT %s FROM %s;", strings.Join(columns, ","), table)
	rows, err := db.Query(sql)
	if err != nil {
		fmt.Printf("\n\t[Error] Extracting columns from %s: %v", table, err)
	}
	defer rows.Close()
	return toSlice(rows)
}


func GetNumOccurances(db *sql.DB, table, column string) map[string]int {
	// Returns map with number of unique entries in column
	occ := make(map[string]int)
	entries := GetColumnText(db, table, column)
	for _, i := range entries {
		if strarray.InMapStrInt(occ, i) == true {
			occ[i]++
		} else {
			occ[i] = 1
		}
	}
	return occ
}

func GetTable(db *sql.DB, table string) [][]string {
	// Returns contents of table
	sql := fmt.Sprintf("SELECT * FROM %s ;", table)
	rows, err := db.Query(sql)
	if err != nil {
		fmt.Printf("\n\t[Error] Extracting %s: %v", table, err)
	}
	defer rows.Close()
	return toSlice(rows)
}

func GetTableMap(db *sql.DB, table string) map[string][]string {
	// Returns table as a map with id as the key
	tbl := make(map[string][]string)
	s := GetTable(db, table)
	for _, i := range s {
		tbl[i[0]] = i[1:]
	}
	return tbl
}
