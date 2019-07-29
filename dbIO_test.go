// Tests dbIO functions which do not access sql database

package dbIO

import (
	"strings"
	"testing"
)

func TestEscapeChars(t *testing.T) {
	// Tests escapeChars (in upload.go) function using raw strings
	matches := []struct {
		input    string
		expected string
	}{
		{"N/A", "NA"},
		{"Na", "NA"},
		{"black_footed_ferret", `black\_footed\_ferret`},
		{"weasel: 'Fred'", `weasel: \'Fred\'`},
		{`badger\ "Reggie" `, `badger- \"Reggie\" `},
	}
	for _, i := range matches {
		actual := escapeChars(i.input)
		if actual != i.expected {
			t.Errorf("Actual excaped value %s is not equal to expected: %s", actual, i.expected)
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
		t.Errorf("Actual length from map %s is not equal to expected: %s", string(aclen), string(exlen))
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
					t.Errorf("Actual string from map %s is not equal to expected: %s", i, j)
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
		t.Errorf("Actual length from slice %s is not equal to expected: %s", string(aclen), string(exlen))
	} else if actual != expected {
		t.Errorf("Actual string from slice %s is not equal to expected: %s", actual, expected)
	}
}

func TestAddApostrophes(t *testing.T) {
	// Tests addApostrophes function (in extract.go)
	matches := []struct {
		input    string
		expected string
	}{
		{"1,Weasel,15", "'1','Weasel','15'"},
		{"2,stoat,9", "'2','stoat','9'"},
		{"3,egret,NA", "'3','egret','NA'"},
		{`4,black\_footed\_ferret,20`, `'4','black\_footed\_ferret','20'`},
	}
	for _, i := range matches {
		actual := addApostrophes(i.input)
		if actual != i.expected {
			t.Errorf("Actual apostrophe string %s is not equal to expected: %s", actual, i.expected)
		}
	}
}
