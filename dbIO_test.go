// Tests dbIO functions which do not access sql database

package dbIO

import (
	"fmt"
	"testing"
)

func fmtMessage(field, a, e string) string {
	// Returns formatted string
	return fmt.Sprintf("Actual %s %s is not equal to expected: %s", field, a, e)
}

func TestColumnEqualTo(t *testing.T) {
	// Tests columnEqualTo function
	values := [][]string{
			{"1","Lion","12"},
			{"2","Tiger",""},
			{"3","","6"},
			{"Leopard","5"},
		}
	expected := []string{"ID='1',Name='Lion',Age='12", "ID='2',Name='Tiger'", "ID='3',,Age='6"}
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
