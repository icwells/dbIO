// These functions will upload data to a database

package dbIO

import (
	"bufio"
	"bytes"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"unicode/utf8"
)

func (d *DBIO) UpdateDB(table, values string, l int) int {
	// Adds new rows to table
	//(values must be formatted for single/multiple rows before calling function)
	cmd, err := d.DB.Prepare(fmt.Sprintf("INSERT INTO %s (%s) VALUES %s;", table, d.Columns[table], values))
	if err != nil {
		fmt.Printf("\t[Error] Formatting command for upload to %s: %v\n", table, err)
		return 0
	}
	_, err = cmd.Exec()
	cmd.Close()
	if err != nil {
		fmt.Printf("\t[Error] Uploading to %s: %v\n", table, err)
		return 0
	}
	fmt.Printf("\tUploaded %d rows to %s.\n", l, table)
	return 1
}

func escapeChars(v string) string {
	// Returns value with any reserved characters escaped and standarizes NAs
	chars := []string{"'", "\"", "_"}
	na := []string{"na", "Na", "N/A"}
	// Reset backslashes to dashes
	v = strings.Replace(v, `\`, "-", -1)
	for _, i := range chars {
		idx := 0
		for strings.Contains(v[idx:], i) == true {
			// Escape each occurance of a character
			ind := strings.Index(v[idx:], i)
			idx = idx + ind
			v = v[:idx] + `\` + v[idx:]
			idx++
			idx++
		}
	}
	for _, i := range na {
		// Standardize NA values
		if strings.Contains(v, i) == true {
			v = strings.Replace(v, i, "NA", -1)
		}
	}
	return v
}

func validateString(v string) string {
	// Returns valid string for upload to database
	if _, err := strconv.Atoi(v); err != nil {
		// Avoid assigning NA to numerical value
		if utf8.ValidString(v) == false || strings.Contains(v, `\xEF\xBF\xBD`) == true {
			v = "NA"
		}
	}
	return escapeChars(v)
}

func FormatMap(data map[string][]string) (string, int) {
	// Formats a map of string slices for upload
	buffer := bytes.NewBufferString("")
	first := true
	count := 0
	for _, val := range data {
		f := true
		if first == false {
			// Add sepearating comma
			buffer.WriteByte(',')
		}
		buffer.WriteByte('(')
		for _, v := range val {
			if f == false {
				buffer.WriteByte(',')
			}
			// Wrap in apostrophes to preserve spaces and reserved characters
			buffer.WriteByte('\'')
			buffer.WriteString(validateString(v))
			buffer.WriteByte('\'')
			f = false
		}
		buffer.WriteByte(')')
		first = false
		count++
	}
	return buffer.String(), count
}

func FormatSlice(data [][]string) (string, int) {
	// Organizes input data into n rows for upload
	buffer := bytes.NewBufferString("")
	count := 0
	for idx, row := range data {
		if idx != 0 {
			buffer.WriteByte(',')
		}
		buffer.WriteByte('(')
		for i, v := range row {
			if i != 0 {
				buffer.WriteByte(',')
			}
			// Wrap in apostrophes to preserve spaces and reserved characters
			buffer.WriteByte('\'')
			buffer.WriteString(validateString(v))
			buffer.WriteByte('\'')
		}
		buffer.WriteByte(')')
		count++
	}
	return buffer.String(), count
}

func openFile(file string) *os.File {
	// Returns file stream, exits if it encounters an error
	f, err := os.Open(file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n\t[ERROR] Reading %s: %v\n\n", file, err)
		os.Exit(10)
	}
	return f
}

func (d *DBIO) columnMap(rows *sql.Rows) {
	// Converts sql query result to map of strings
	columns, _ := rows.Columns()
	count := len(columns)
	values := make([]interface{}, count)
	pointers := make([]interface{}, count)
	for rows.Next() {
		for i, _ := range columns {
			pointers[i] = &values[i]
		}
		// Maps items to values via pointers
		rows.Scan(pointers...)
		fmt.Println(pointers)
		// Use Sprintf to convert interface to string
		k := fmt.Sprintf("%s", values[0])
		v := fmt.Sprintf("%s", values[1])
		d.Columns[k] = v
	}
	fmt.Println(d.Columns)
}

func (d *DBIO) GetTableColumns() {
	// Extracts tables and columns from database and stores in Columns map
	d.Columns = make(map[string]string)
	cmd := `SELECT table_name,GROUP_CONCAT(column_name ORDER BY ordinal_position) FROM information_schema.columns 
WHERE table_schema = DATABASE() GROUP BY table_name ORDER BY table_name;`
	rows, err := d.DB.Query(cmd)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n\t[ERROR] Extracting table and column names: %v\n\n", err)
	}
	defer rows.Close()
	d.columnMap(rows)
}

func (d *DBIO) ReadColumns(infile string) {
	// Build map of column statements
	d.Columns = make(map[string]string)
	var table string
	f := openFile(infile)
	defer f.Close()
	input := bufio.NewScanner(f)
	for input.Scan() {
		line := string(input.Text())
		if len(line) >= 3 {
			if line[0] == '#' {
				// Get table names
				table = strings.TrimSpace(line[1:])
			} else {
				// Get columns for given table
				col := strings.TrimSpace(line)
				if _, ex := d.Columns[table]; ex == true {
					d.Columns[table] = d.Columns[table] + ", " + col
				} else {
					d.Columns[table] = col
				}
			}
		}
	}
}

func (d *DBIO) NewTables(infile string) {
	// Initializes new tables
	fmt.Println("\n\tInitializing new tables...")
	d.ReadColumns(infile)
	for k, v := range d.Columns {
		cmd := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(%s);", k, v)
		_, err := d.DB.Exec(cmd)
		if err != nil {
			fmt.Printf("\t[Error] Creating table %s. %v\n\n", k, err)
			os.Exit(1)
		}
	}
}
