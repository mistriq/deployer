package app

import "testing"

func TestPostgresBindSkipsQuotedQuestionMarks(t *testing.T) {
	got := bind("SELECT '?', 'it''s ?', \"?\", value -- ?\nFROM records /* ? */ WHERE first=? AND second=?", "postgres")
	want := "SELECT '?', 'it''s ?', \"?\", value -- ?\nFROM records /* ? */ WHERE first=$1 AND second=$2"
	if got != want {
		t.Fatalf("bind = %q, want %q", got, want)
	}
}
