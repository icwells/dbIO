// Tests dbIO functions which do not access sql database

package dbIO

import (
	"fmt"
	"strings"
	"testing"
)

func fmtMessage(field, a, e string) string {
	// Returns formatted string
	return fmt.Sprintf("Actual %s %s is not equal to expected: %s", field, a, e)
}

func TestColumnEqualTo(t *testing.T) {
	// Tests columnEqualTo function (in update.go)
	values := [][]string{
			{"1","Lion","12"},
			{"2","Tiger",""},
			{"3","","6"},
			{"Leopard","5"},
		}
	expected := []string{"ID='1',Name='Lion',Age='12", "ID='2',Name='Tiger'", "ID='3,Age='6"}
	actual := columnEqualTo("ID,Name,Age", values)
	if len(actual) != len(expected) {
		msg := fmtMessage("length", string(len(actual)), string(len(expected)))
		t.Error(msg)
	}
	for idx, i := range actual {
		if len(i) == 3 && i != expected[idx] {
			msg := fmtMessage("row", i, expected[idx])
			t.Error(msg)
		}
	}
}

func TestEscapeChars(t *testing.T) {
	// Tests escapeChars (in upload.go) function using raw strings
	matches := []struct{
		input		string
		expected	string
	} {
		{"N/A", "NA"},
		{"Na", "NA"},
		{"black_footed_ferret", `black\_footed\_ferret`},
		{"weasel: 'Fred'", `weasel: \'Fred\'`},
		{`badger\ "Reggie" `, `badger- \"Reggie\" `},
	}
	for _, i := range matches {
		actual := escapeChars(i.input)
		if actual != i.expected {
			msg := fmtMessage("escaped value", actual, i.expected)
			t.Error(msg)
		}
	}
}

func getReturnString() (string, int) {
	// Returns expected result of FormatMap and FormatSlice
	l := 4
	s := `('1','Weasel','15'),('2','stoat','9'),('3','egret','NA'),('4','black\_footed\_ferret','20')`
	return s, l
}

func TestFormatMap(t *testing.T) {
	// Tests FormatMap (in upload.go)
	expected, exlen := getReturnString()
	values := map[string][]string{
		"a": {"1", "Weasel", "15"},
		"b": {"2", "stoat", "9"},
		"c": {"3", "egret", "na"},
		"d": {"4", "black_footed_ferret", "20"},
	}
	actual, aclen := FormatMap(values)
	if aclen != exlen {
		msg := fmtMessage("length from map", string(aclen), string(exlen))
		t.Error(msg)
	}
	// Compare individual elements to account for random order of map
	a := strings.Split(actual, "),(")
	e := strings.Split(expected, "),(")
	for _, i := range a {
		i = strings.Replace(i, "(", "", -1)
		i = strings.Replace(i, ")", "", -1)
		id := strings.Split(i, ",")[0]
		for _, j := range e {
			j = strings.Replace(j, "(", "", -1)
			j = strings.Replace(j, ")", "", -1)
			if id == strings.Split(j, ",")[0] {
				if i != j {		
					msg := fmtMessage("string from map", i, j)
					t.Error(msg)
				}
				break
			}
		}
	}
}

func TestFormatSlice(t *testing.T) {
	// Tests FormatSLice (in upload.go)
	expected, exlen := getReturnString()
	values := [][]string{
		{"1", "Weasel", "15"},
		{"2", "stoat", "9"},
		{"3", "egret", "na"},
		{"4", "black_footed_ferret", "20"},
	}
	actual, aclen := FormatSlice(values)
	if aclen != exlen {
		msg := fmtMessage("length from slice", string(aclen), string(exlen))
		t.Error(msg)
	} else if actual != expected {
		msg := fmtMessage("string from slice", actual, expected)
		t.Error(msg)
	}
}

func TestAddApostrophes(t *testing.T) {
	// Tests addApostrophes function (in extract.go)
	matches := []struct{
		input		string
		expected	string
	} {
		{"1,Weasel,15", "'1','Weasel','15'"},
		{"2,stoat,9", "'2','stoat','9'"},
		{"3,egret,NA", "'3','egret','NA'"},
		{`4,black\_footed\_ferret,20`, `'4','black\_footed\_ferret','20'`},
	}
	for _, i := range matches {
		actual := addApostrophes(i.input)
		if actual != i.expected {
			msg := fmtMessage("apostrophe string", actual, i.expected)
			t.Error(msg)
		}
	}
}
