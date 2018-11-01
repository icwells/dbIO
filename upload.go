// These functions will upload data to a database

package dbIO

import (
	"bufio"
	"bytes"
	"database/sql"
	"fmt"
	"github.com/icwells/go-tools/iotools"
	"github.com/icwells/go-tools/strarray"
	"os"
	"strings"
)

func UpdateDB(db *sql.DB, table, columns, values string, l int) int {
	// Adds new rows to table
	//(values must be formatted for single/multiple rows before calling function)
	cmd, err := db.Prepare(fmt.Sprintf("INSERT INTO %s (%s) VALUES %s;", table, columns, values))
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
	// Returns value with any reserved characters escaped and NAs converted to Null
	chars := []string{"'", "\"", "_"}
	na := []string{"Na", "N/A"}
	// Reset semicolons to commas
	v = strings.Replace(v, "\\", "-", -1)
	for _, i := range na {
		// Standardize NA values
		if v == i {
			v = strings.Replace(v, i, "NA", -1)
		}
	}
	for _, i := range chars {
		idx := 0
		for strings.Contains(v[idx:], i) == true {
			// Escape each occurance of a character
			ind := strings.Index(v[idx:], i)
			idx = idx + ind
			v = v[:idx] + "\\" + v[idx:]
			idx++
			idx++
		}
	}
	return v
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
			v = escapeChars(v)
			if f == false {
				buffer.WriteByte(',')
			}
			// Wrap in apostrophes to preserve spaces and reserved characters
			buffer.WriteByte('\'')
			buffer.WriteString(v)
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
			v = escapeChars(v)
			if i != 0 {
				buffer.WriteByte(',')
			}
			// Wrap in apostrophes to preserve spaces and reserved characters
			buffer.WriteByte('\'')
			buffer.WriteString(v)
			buffer.WriteByte('\'')
		}
		buffer.WriteByte(')')
		count++
	}
	return buffer.String(), count
}

func ReadColumns(infile string, types bool) map[string]string {
	// Build map of column statements
	columns := make(map[string]string)
	var table string
	f := iotools.OpenFile(infile)
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
				var col string
				if types == true {
					col = strings.TrimSpace(line)
				} else {
					c := strings.Split(line, " ")
					col = strings.TrimSpace(c[0])
				}
				if strarray.InMapStr(columns, table) == true {
					columns[table] = columns[table] + ", " + col
				} else {
					columns[table] = col
				}
			}
		}
	}
	return columns
}

func NewTables(db *sql.DB, infile string) {
	// Initializes new tables
	fmt.Println("\n\tInitializing new tables...")
	columns := ReadColumns(infile, true)
	for k, v := range columns {
		cmd := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s(%s);", k, v)
		_, err := db.Exec(cmd)
		if err != nil {
			fmt.Printf("\t[Error] Creating table %s. %v\n\n", k, err)
			os.Exit(1)
		}
	}
}
