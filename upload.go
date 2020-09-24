// These functions will upload data to a database

package dbIO

import (
	"bufio"
	"bytes"
	"database/sql"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Insert executes the given INSERT command
func (d *DBIO) Insert(table, command string) error {
	var err error
	cmd, err := d.DB.Prepare(command)
	if err != nil {
		d.logger.Printf("[Error] Formatting command for upload to %s: %v\n", table, err)
	}
	_, err = cmd.Exec()
	cmd.Close()
	if err != nil {
		d.logger.Printf("[Error] Uploading to %s: %v\n", table, err)
	}
	return err
}

func getDenominator(list [][]string) int {
	// Returns denominator for subsetting upload slice (size in bytes / 16Mb)
	max := 10000000.0
	size := 0
	for _, i := range list {
		for _, j := range i {
			size += len([]byte(j))
		}
	}
	return int(math.Ceil(float64(size*8) / max))
}

// UploadSlice formats two-dimensional string slice for upload to database and splits uploads into chunks if it exceeds SQL size limit.
func (d *DBIO) UploadSlice(table string, values [][]string) error {
	var err error
	if len(values) > 0 {
		// Upload in chunks
		idx := len(values) / getDenominator(values)
		var start, end int
		for start < len(values)-idx {
			// Advance indeces
			start += idx
			end = start + idx
			if end > len(values) {
				// Get last less than idx rows
				end = len(values)
			}
			vals, _ := FormatSlice(values[start:end])
			err = d.Insert(table, fmt.Sprintf("INSERT INTO %s (%s) VALUES %s;", table, d.Columns[table], vals))
			if err == nil {
				fmt.Printf("\r\tUploaded %d of %d rows to %s.", end, len(values), table)
			} else {
				break
			}
		}
		fmt.Println()
	}
	return err
}

// UpdateDB adds new rows to table. Values must be formatted using FormatMap or FormatSlice.
func (d *DBIO) UpdateDB(table, values string, l int) int {
	cmd := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s;", table, d.Columns[table], values)
	err := d.Insert(table, cmd)
	if err != nil {
		return 0
	}
	fmt.Printf("\tUploaded %d rows to %s.\n", l, table)
	return 1
}

// Returns value with any reserved characters escaped and standarizes NAs.
func escapeChars(v string) string {
	chars := []string{"'", "\"", "_"}
	na := []string{" na ", " Na ", "N/A"}
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
		} else if strings.TrimSpace(i) == strings.TrimSpace(v) {
			v = "NA"
		}
	}
	return v
}

// Returns valid string for upload to database.
func validateString(v string) string {
	if _, err := strconv.Atoi(v); err != nil {
		// Avoid assigning NA to numerical value
		if utf8.ValidString(v) == false || strings.Contains(v, `\xEF\xBF\xBD`) == true {
			v = "NA"
		}
	}
	return escapeChars(v)
}

// FormatMap converts a map of string slices to a string formatted with parentheses, commas, and appostrophe's where needed. Returns the number of rows formatted.
func FormatMap(data map[string][]string) (string, int) {
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

// FormatSlice converts a two-dimensional string slice to a string formatted with parentheses, commas, and appostrophe's where needed. Returns the number of rows formatted.
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

// Converts sql query result to map of strings.
func (d *DBIO) columnMap(rows *sql.Rows) {
	columns, _ := rows.Columns()
	count := len(columns)
	values := make([]interface{}, count)
	pointers := make([]interface{}, count)
	for rows.Next() {
		for i := range columns {
			pointers[i] = &values[i]
		}
		// Maps items to values via pointers
		rows.Scan(pointers...)
		// Use Sprintf to convert interface to string
		k := fmt.Sprintf("%s", values[0])
		v := fmt.Sprintf("%s", values[1])
		d.Columns[k] = v
	}
}

// GetTableColumns extracts table and column names from the database and stores them in the Columns map.
func (d *DBIO) GetTableColumns() {
	d.Columns = make(map[string]string)
	cmd := `SELECT table_name,GROUP_CONCAT(column_name ORDER BY ordinal_position) FROM information_schema.columns 
WHERE table_schema = DATABASE() GROUP BY table_name ORDER BY table_name;`
	rows, err := d.DB.Query(cmd)
	if err != nil {
		d.logger.Printf("[ERROR] Extracting table and column names: %v\n\n", err)
	}
	defer rows.Close()
	d.columnMap(rows)
}

// ReadColumns builds a map of column statements with types from infile. See README for infile formatting.
func (d *DBIO) ReadColumns(infile string) {
	d.Columns = make(map[string]string)
	var table string
	f, err := os.Open(infile)
	if err != nil {
		d.logger.Fatalf("[ERROR] Reading %s: %v\n\n", infile, err)
	}
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

// NewTables initializes new tables form infile. See README for infile formatting.
func (d *DBIO) NewTables(infile string) {
	fmt.Println("\n\tInitializing new tables...")
	d.ReadColumns(infile)
	for k, v := range d.Columns {
		cmd := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(%s);", k, v)
		_, err := d.DB.Exec(cmd)
		if err != nil {
			d.logger.Fatalf("[Error] Creating table %s. %v\n\n", k, err)
		}
	}
}
