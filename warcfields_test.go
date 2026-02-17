package gowarc

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNameValue_String(t *testing.T) {
	tests := []struct {
		name string
		nv   nameValue
		want string
	}{
		{"simple field", nameValue{"Content-Type", "text/html"}, "Content-Type: text/html"},
		{"empty value", nameValue{"X-Empty", ""}, "X-Empty: "},
		{"warc field", nameValue{WarcRecordID, "<urn:uuid:1234>"}, "WARC-Record-ID: <urn:uuid:1234>"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.nv.String())
		})
	}
}

func TestWarcFields_CanonicalHeaderKey(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"lowercase", "content-type", "Content-Type"},
		{"uppercase", "CONTENT-TYPE", "Content-Type"},
		{"mixed", "cOnTeNt-TyPe", "Content-Type"},
		{"single word", "host", "Host"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wf := &WarcFields{}
			assert.Equal(t, tt.want, wf.CanonicalHeaderKey(tt.input))
		})
	}
}

func TestWarcFields_Add(t *testing.T) {
	tests := []struct {
		name       string
		initial    WarcFields
		fieldName  string
		fieldValue string
		want       WarcFields
	}{
		{"Add to empty",
			WarcFields{},
			"name1", "value1",
			WarcFields{&nameValue{"Name1", "value1"}}},
		{"Add new field",
			WarcFields{&nameValue{"Name1", "value1"}},
			"name2", "value2",
			WarcFields{&nameValue{"Name1", "value1"}, &nameValue{"Name2", "value2"}}},
		{"Add same field",
			WarcFields{&nameValue{"Name1", "value1"}},
			"name1", "value2",
			WarcFields{&nameValue{"Name1", "value1"}, &nameValue{"Name1", "value2"}}},
		{"Add same field, well known field name",
			WarcFields{&nameValue{"WARC-Record-ID", "value1"}},
			"warc-record-id", "value2",
			WarcFields{&nameValue{"WARC-Record-ID", "value1"}, &nameValue{"WARC-Record-ID", "value2"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.initial.Add(tt.fieldName, tt.fieldValue)
			assert.ElementsMatch(t, tt.want, tt.initial)
		})
	}
}

func TestWarcFields_AddId(t *testing.T) {
	tests := []struct {
		name       string
		initial    WarcFields
		fieldName  string
		fieldValue string
		want       WarcFields
	}{
		{"Add to empty",
			WarcFields{},
			"name1", "value1",
			WarcFields{&nameValue{"Name1", "<value1>"}}},
		{"Add preformatted",
			WarcFields{},
			"name1", "<value1>",
			WarcFields{&nameValue{"Name1", "<value1>"}}},
		{"Add new field",
			WarcFields{&nameValue{"Name1", "<value1>"}},
			"name2", "value2",
			WarcFields{&nameValue{"Name1", "<value1>"}, &nameValue{"Name2", "<value2>"}}},
		{"Add same field",
			WarcFields{&nameValue{"Name1", "<value1>"}},
			"name1", "value2",
			WarcFields{&nameValue{"Name1", "<value1>"}, &nameValue{"Name1", "<value2>"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.initial.AddId(tt.fieldName, tt.fieldValue)
			assert.ElementsMatch(t, tt.want, tt.initial)
		})
	}
}

func TestWarcFields_AddInt(t *testing.T) {
	tests := []struct {
		name       string
		initial    WarcFields
		fieldName  string
		fieldValue int
		want       WarcFields
	}{
		{"Add to empty",
			WarcFields{},
			"name1", 1,
			WarcFields{&nameValue{"Name1", "1"}}},
		{"Add new field",
			WarcFields{&nameValue{"Name1", "1"}},
			"name2", 2,
			WarcFields{&nameValue{"Name1", "1"}, &nameValue{"Name2", "2"}}},
		{"Add same field",
			WarcFields{&nameValue{"Name1", "1"}},
			"name1", 2,
			WarcFields{&nameValue{"Name1", "1"}, &nameValue{"Name1", "2"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.initial.AddInt(tt.fieldName, tt.fieldValue)
			assert.ElementsMatch(t, tt.want, tt.initial)
		})
	}
}

func TestWarcFields_AddInt64(t *testing.T) {
	tests := []struct {
		name       string
		initial    WarcFields
		fieldName  string
		fieldValue int64
		want       WarcFields
	}{
		{"Add to empty",
			WarcFields{},
			"name1", 1,
			WarcFields{&nameValue{"Name1", "1"}}},
		{"Add new field",
			WarcFields{&nameValue{"Name1", "1"}},
			"name2", 2,
			WarcFields{&nameValue{"Name1", "1"}, &nameValue{"Name2", "2"}}},
		{"Add same field",
			WarcFields{&nameValue{"Name1", "1"}},
			"name1", 2,
			WarcFields{&nameValue{"Name1", "1"}, &nameValue{"Name1", "2"}}},
		{"Add zero",
			WarcFields{},
			"Test-Field", 0,
			WarcFields{&nameValue{"Test-Field", "0"}}},
		{"Add negative",
			WarcFields{},
			"Test-Field", -42,
			WarcFields{&nameValue{"Test-Field", "-42"}}},
		{"Add max int64",
			WarcFields{},
			"Test-Field", 9223372036854775807,
			WarcFields{&nameValue{"Test-Field", "9223372036854775807"}}},
		{"Add min int64",
			WarcFields{},
			"Test-Field", -9223372036854775808,
			WarcFields{&nameValue{"Test-Field", "-9223372036854775808"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.initial.AddInt64(tt.fieldName, tt.fieldValue)
			assert.ElementsMatch(t, tt.want, tt.initial)
		})
	}
}

func TestWarcFields_AddTime(t *testing.T) {
	tests := []struct {
		name       string
		initial    WarcFields
		fieldName  string
		fieldValue time.Time
		want       WarcFields
	}{
		{"Add to empty",
			WarcFields{},
			"name1", time.Date(2021, 12, 25, 18, 30, 0, 20, time.UTC),
			WarcFields{&nameValue{"Name1", "2021-12-25T18:30:00Z"}}},
		{"Add new field",
			WarcFields{&nameValue{"Name1", "2021-12-25T18:30:00Z"}},
			"name2", time.Date(2022, 12, 25, 18, 30, 10, 20, time.UTC),
			WarcFields{&nameValue{"Name1", "2021-12-25T18:30:00Z"}, &nameValue{"Name2", "2022-12-25T18:30:10Z"}}},
		{"Add same field",
			WarcFields{&nameValue{"Name1", "2021-12-25T18:30:00Z"}},
			"name1", time.Date(2022, 12, 25, 18, 30, 10, 20, time.UTC),
			WarcFields{&nameValue{"Name1", "2021-12-25T18:30:00Z"}, &nameValue{"Name1", "2022-12-25T18:30:10Z"}}},
		{"Add UTC time",
			WarcFields{},
			"WARC-Date", time.Date(2020, time.January, 5, 10, 44, 25, 0, time.UTC),
			WarcFields{&nameValue{"WARC-Date", "2020-01-05T10:44:25Z"}}},
		{"Add non-UTC time converted to UTC",
			WarcFields{},
			"WARC-Date", time.Date(2020, time.January, 5, 10, 44, 25, 0, time.FixedZone("EST", -5*3600)),
			WarcFields{&nameValue{"WARC-Date", "2020-01-05T15:44:25Z"}}},
		{"Add zero time",
			WarcFields{},
			"WARC-Date", time.Time{},
			WarcFields{&nameValue{"WARC-Date", "0001-01-01T00:00:00Z"}}},
		{"Add time with nanoseconds truncated",
			WarcFields{},
			"WARC-Date", time.Date(2020, time.January, 5, 10, 44, 25, 123456789, time.UTC),
			WarcFields{&nameValue{"WARC-Date", "2020-01-05T10:44:25Z"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.initial.AddTime(tt.fieldName, tt.fieldValue)
			assert.ElementsMatch(t, tt.want, tt.initial)
		})
	}
}

func TestWarcFields_Delete(t *testing.T) {
	tests := []struct {
		name      string
		initial   WarcFields
		fieldName string
		want      WarcFields
	}{
		{"Delete from empty",
			WarcFields{},
			"name1",
			WarcFields{}},
		{"Delete one field",
			WarcFields{&nameValue{"Name1", "value1"}},
			"name1",
			WarcFields{}},
		{"Delete non existing field",
			WarcFields{&nameValue{"Name1", "value1"}},
			"name2",
			WarcFields{&nameValue{"Name1", "value1"}}},
		{"Delete field with two values",
			WarcFields{&nameValue{"Name1", "value1"}, &nameValue{"Name1", "value2"}},
			"name1",
			WarcFields{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.initial.Delete(tt.fieldName)
			assert.ElementsMatch(t, tt.want, tt.initial)
		})
	}
}

func TestWarcFields_Get(t *testing.T) {
	tests := []struct {
		name      string
		initial   WarcFields
		fieldName string
		want      string
	}{
		{"Get from empty",
			WarcFields{},
			"name1",
			""},
		{"Get one field",
			WarcFields{&nameValue{"Name1", "value1"}},
			"name1",
			"value1"},
		{"Get non existing field",
			WarcFields{&nameValue{"Name1", "value1"}},
			"name2",
			""},
		{"Get field with two values",
			WarcFields{&nameValue{"Name1", "value1"}, &nameValue{"Name1", "value2"}},
			"name1",
			"value1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.initial.Get(tt.fieldName)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestWarcFields_GetAll(t *testing.T) {
	tests := []struct {
		name      string
		initial   WarcFields
		fieldName string
		want      []string
	}{
		{"Get from empty",
			WarcFields{},
			"name1",
			nil},
		{"Get one field",
			WarcFields{&nameValue{"Name1", "value1"}},
			"name1",
			[]string{"value1"}},
		{"Get non existing field",
			WarcFields{&nameValue{"Name1", "value1"}},
			"name2",
			nil},
		{"Get field with two values",
			WarcFields{&nameValue{"Name1", "value1"}, &nameValue{"Name1", "value2"}},
			"name1",
			[]string{"value1", "value2"}},
		{"Get field with three values",
			WarcFields{&nameValue{"Test-Field", "value1"}, &nameValue{"Test-Field", "value2"}, &nameValue{"Test-Field", "value3"}, &nameValue{"Other-Field", "other"}},
			"Test-Field",
			[]string{"value1", "value2", "value3"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.initial.GetAll(tt.fieldName)
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestWarcFields_GetId(t *testing.T) {
	tests := []struct {
		name      string
		initial   WarcFields
		fieldName string
		want      string
	}{
		{"Get from empty",
			WarcFields{},
			"name1",
			""},
		{"Get one field",
			WarcFields{&nameValue{"Name1", "<value1>"}},
			"name1",
			"value1"},
		{"Get non existing field",
			WarcFields{&nameValue{"Name1", "<value1>"}},
			"name2",
			""},
		{"Get field with two values",
			WarcFields{&nameValue{"Name1", "<value1>"}, &nameValue{"Name1", "<value2>"}},
			"name1",
			"value1"},
		{"ID with angle brackets",
			WarcFields{&nameValue{"WARC-Record-ID", "<urn:uuid:12345>"}},
			"WARC-Record-ID",
			"urn:uuid:12345"},
		{"ID without angle brackets",
			WarcFields{&nameValue{"WARC-Record-ID", "urn:uuid:12345"}},
			"WARC-Record-ID",
			"urn:uuid:12345"},
		{"ID with only opening bracket",
			WarcFields{&nameValue{"WARC-Record-ID", "<urn:uuid:12345"}},
			"WARC-Record-ID",
			"urn:uuid:12345"},
		{"ID with only closing bracket",
			WarcFields{&nameValue{"WARC-Record-ID", "urn:uuid:12345>"}},
			"WARC-Record-ID",
			"urn:uuid:12345"},
		{"empty ID",
			WarcFields{&nameValue{"WARC-Record-ID", ""}},
			"WARC-Record-ID",
			""},
		{"ID with just brackets",
			WarcFields{&nameValue{"WARC-Record-ID", "<>"}},
			"WARC-Record-ID",
			""},
		{"ID with multiple brackets",
			WarcFields{&nameValue{"WARC-Record-ID", "<<urn:uuid:12345>>"}},
			"WARC-Record-ID",
			"urn:uuid:12345"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.initial.GetId(tt.fieldName)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestWarcFields_GetInt(t *testing.T) {
	tests := []struct {
		name      string
		initial   WarcFields
		fieldName string
		want      int
		wantErr   bool
	}{
		{"Get from empty",
			WarcFields{},
			"name1",
			0, true},
		{"Get one field",
			WarcFields{&nameValue{"Name1", "1"}},
			"name1",
			1, false},
		{"Get empty field",
			WarcFields{&nameValue{"Name1", ""}},
			"name1",
			0, true},
		{"Get badly formatted field",
			WarcFields{&nameValue{"Name1", "value1"}},
			"name1",
			0, true},
		{"Get non existing field",
			WarcFields{&nameValue{"Name1", "<value1>"}},
			"name2",
			0, true},
		{"Get field with two values",
			WarcFields{&nameValue{"Name1", "1"}, &nameValue{"Name1", "2"}},
			"name1",
			1, false},
		{"valid positive integer",
			WarcFields{&nameValue{"Content-Length", "42"}},
			"Content-Length",
			42, false},
		{"valid zero",
			WarcFields{&nameValue{"Content-Length", "0"}},
			"Content-Length",
			0, false},
		{"valid negative integer",
			WarcFields{&nameValue{"Test-Field", "-42"}},
			"Test-Field",
			-42, false},
		{"large integer",
			WarcFields{&nameValue{"Test-Field", "2147483647"}}, // Max int32
			"Test-Field",
			2147483647, false},
		{"float value",
			WarcFields{&nameValue{"Content-Length", "42.5"}},
			"Content-Length",
			0, true},
		{"value with spaces",
			WarcFields{&nameValue{"Content-Length", " 42 "}},
			"Content-Length",
			0, true},
		{"hexadecimal value",
			WarcFields{&nameValue{"Content-Length", "0x2A"}},
			"Content-Length",
			0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.initial.GetInt(tt.fieldName)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestWarcFields_GetInt64(t *testing.T) {
	tests := []struct {
		name      string
		initial   WarcFields
		fieldName string
		want      int64
		wantErr   bool
	}{
		{"Get from empty",
			WarcFields{},
			"name1",
			0, true},
		{"Get one field",
			WarcFields{&nameValue{"Name1", "1"}},
			"name1",
			1, false},
		{"Get empty field",
			WarcFields{&nameValue{"Name1", ""}},
			"name1",
			0, true},
		{"Get badly formatted field",
			WarcFields{&nameValue{"Name1", "value1"}},
			"name1",
			0, true},
		{"Get non existing field",
			WarcFields{&nameValue{"Name1", "<value1>"}},
			"name2",
			0, true},
		{"Get field with two values",
			WarcFields{&nameValue{"Name1", "1"}, &nameValue{"Name1", "2"}},
			"name1",
			1, false},
		{"valid positive integer",
			WarcFields{&nameValue{"Content-Length", "42"}},
			"Content-Length",
			42, false},
		{"valid zero",
			WarcFields{&nameValue{"Content-Length", "0"}},
			"Content-Length",
			0, false},
		{"large int64",
			WarcFields{&nameValue{"Test-Field", "9223372036854775807"}}, // Max int64
			"Test-Field",
			9223372036854775807, false},
		{"negative int64",
			WarcFields{&nameValue{"Test-Field", "-9223372036854775808"}}, // Min int64
			"Test-Field",
			-9223372036854775808, false},
		{"overflow int64",
			WarcFields{&nameValue{"Test-Field", "9223372036854775808"}}, // Max int64 + 1
			"Test-Field",
			0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.initial.GetInt64(tt.fieldName)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestWarcFields_GetTime(t *testing.T) {
	tests := []struct {
		name      string
		initial   WarcFields
		fieldName string
		want      time.Time
		wantErr   bool
	}{
		{"Get from empty",
			WarcFields{},
			"name1",
			time.Time{}, true},
		{"Get one field",
			WarcFields{&nameValue{"Name1", "2021-12-25T18:30:00Z"}},
			"name1",
			time.Date(2021, 12, 25, 18, 30, 0, 0, time.UTC), false},
		{"Get empty field",
			WarcFields{&nameValue{"Name1", ""}},
			"name1",
			time.Time{}, true},
		{"Get badly formatted field",
			WarcFields{&nameValue{"Name1", "value1"}},
			"name1",
			time.Time{}, true},
		{"Get non existing field",
			WarcFields{&nameValue{"Name1", "<value1>"}},
			"name2",
			time.Time{}, true},
		{"Get field with two values",
			WarcFields{&nameValue{"Name1", "2021-12-25T18:30:00Z"}, &nameValue{"Name1", "2022-12-25T18:30:00Z"}},
			"name1",
			time.Date(2021, 12, 25, 18, 30, 0, 0, time.UTC), false},
		{"valid RFC3339 timestamp",
			WarcFields{&nameValue{"WARC-Date", "2020-01-05T10:44:25Z"}},
			"WARC-Date",
			time.Date(2020, time.January, 5, 10, 44, 25, 0, time.UTC), false},
		{"valid RFC3339 with timezone",
			WarcFields{&nameValue{"WARC-Date", "2020-01-05T10:44:25+01:00"}},
			"WARC-Date",
			time.Date(2020, time.January, 5, 9, 44, 25, 0, time.UTC), false},
		{"valid RFC3339 with nanoseconds",
			WarcFields{&nameValue{"WARC-Date", "2020-01-05T10:44:25.123456789Z"}},
			"WARC-Date",
			time.Date(2020, time.January, 5, 10, 44, 25, 123456789, time.UTC), false},
		{"invalid format",
			WarcFields{&nameValue{"WARC-Date", "2020-01-05 10:44:25"}},
			"WARC-Date",
			time.Time{}, true},
		{"14-digit format",
			WarcFields{&nameValue{"WARC-Date", "20200105104425"}},
			"WARC-Date",
			time.Time{}, true},
		{"invalid month",
			WarcFields{&nameValue{"WARC-Date", "2020-13-05T10:44:25Z"}},
			"WARC-Date",
			time.Time{}, true},
		{"invalid day",
			WarcFields{&nameValue{"WARC-Date", "2020-01-32T10:44:25Z"}},
			"WARC-Date",
			time.Time{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.initial.GetTime(tt.fieldName)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				// Use Equal for time comparison to handle different timezones
				if !got.Equal(tt.want) {
					t.Errorf("GetTime(%q) = %v, want %v", tt.fieldName, got, tt.want)
				}
			}
		})
	}
}

func TestWarcFields_Has(t *testing.T) {
	tests := []struct {
		name      string
		initial   WarcFields
		fieldName string
		want      bool
	}{
		{"Has from empty",
			WarcFields{},
			"name1",
			false},
		{"Has one field",
			WarcFields{&nameValue{"Name1", "value1"}},
			"name1",
			true},
		{"Has non existing field",
			WarcFields{&nameValue{"Name1", "value1"}},
			"name2",
			false},
		{"Has field with two values",
			WarcFields{&nameValue{"Name1", "value1"}, &nameValue{"Name1", "value2"}},
			"name1",
			true},
		{"Has case insensitive - uppercase",
			WarcFields{&nameValue{"WARC-Record-ID", "test"}},
			"WARC-Record-ID",
			true},
		{"Has case insensitive - lowercase",
			WarcFields{&nameValue{"WARC-Record-ID", "test"}},
			"warc-record-id",
			true},
		{"Has case insensitive - mixed case",
			WarcFields{&nameValue{"WARC-Record-ID", "test"}},
			"Warc-Record-Id",
			true},
		{"Has case insensitive - all caps",
			WarcFields{&nameValue{"WARC-Record-ID", "test"}},
			"WARC-RECORD-ID",
			true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.initial.Has(tt.fieldName)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestWarcFields_Set(t *testing.T) {
	tests := []struct {
		name       string
		initial    WarcFields
		fieldName  string
		fieldValue string
		want       WarcFields
	}{
		{"Set to empty",
			WarcFields{},
			"name1", "value1",
			WarcFields{&nameValue{"Name1", "value1"}}},
		{"Set new field",
			WarcFields{&nameValue{"Name1", "value1"}},
			"name2", "value2",
			WarcFields{&nameValue{"Name1", "value1"}, &nameValue{"Name2", "value2"}}},
		{"Set existing field",
			WarcFields{&nameValue{"Name1", "value1"}},
			"name1", "value2",
			WarcFields{&nameValue{"Name1", "value2"}}},
		{"Set existing field with two values",
			WarcFields{&nameValue{"Name1", "value1"}, &nameValue{"Name1", "value2"}},
			"name1", "value3",
			WarcFields{&nameValue{"Name1", "value3"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.initial.Set(tt.fieldName, tt.fieldValue)
			assert.ElementsMatch(t, tt.want, tt.initial)
		})
	}
}

func TestWarcFields_SetId(t *testing.T) {
	tests := []struct {
		name       string
		initial    WarcFields
		fieldName  string
		fieldValue string
		want       WarcFields
	}{
		{"Set to empty",
			WarcFields{},
			"name1", "value1",
			WarcFields{&nameValue{"Name1", "<value1>"}}},
		{"Set preformatted",
			WarcFields{},
			"name1", "<value1>",
			WarcFields{&nameValue{"Name1", "<value1>"}}},
		{"Set new field",
			WarcFields{&nameValue{"Name1", "<value1>"}},
			"name2", "value2",
			WarcFields{&nameValue{"Name1", "<value1>"}, &nameValue{"Name2", "<value2>"}}},
		{"Set existing field",
			WarcFields{&nameValue{"Name1", "<value1>"}},
			"name1", "value2",
			WarcFields{&nameValue{"Name1", "<value2>"}}},
		{"Set existing field with two values",
			WarcFields{&nameValue{"Name1", "value1"}, &nameValue{"Name1", "value2"}},
			"name1", "value3",
			WarcFields{&nameValue{"Name1", "<value3>"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.initial.SetId(tt.fieldName, tt.fieldValue)
			assert.ElementsMatch(t, tt.want, tt.initial)
		})
	}
}

func TestWarcFields_SetInt(t *testing.T) {
	tests := []struct {
		name       string
		initial    WarcFields
		fieldName  string
		fieldValue int
		want       WarcFields
	}{
		{"Set to empty",
			WarcFields{},
			"name1", 1,
			WarcFields{&nameValue{"Name1", "1"}}},
		{"Set new field",
			WarcFields{&nameValue{"Name1", "1"}},
			"name2", 2,
			WarcFields{&nameValue{"Name1", "1"}, &nameValue{"Name2", "2"}}},
		{"Set existing field",
			WarcFields{&nameValue{"Name1", "1"}},
			"name1", 2,
			WarcFields{&nameValue{"Name1", "2"}}},
		{"Set existing field with two values",
			WarcFields{&nameValue{"Name1", "1"}, &nameValue{"Name1", "2"}},
			"name1", 3,
			WarcFields{&nameValue{"Name1", "3"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.initial.SetInt(tt.fieldName, tt.fieldValue)
			assert.ElementsMatch(t, tt.want, tt.initial)
		})
	}
}

func TestWarcFields_SetInt64(t *testing.T) {
	tests := []struct {
		name       string
		initial    WarcFields
		fieldName  string
		fieldValue int64
		want       WarcFields
	}{
		{"Set to empty",
			WarcFields{},
			"name1", 1,
			WarcFields{&nameValue{"Name1", "1"}}},
		{"Set new field",
			WarcFields{&nameValue{"Name1", "1"}},
			"name2", 2,
			WarcFields{&nameValue{"Name1", "1"}, &nameValue{"Name2", "2"}}},
		{"Set existing field",
			WarcFields{&nameValue{"Name1", "1"}},
			"name1", 2,
			WarcFields{&nameValue{"Name1", "2"}}},
		{"Set existing field with two values",
			WarcFields{&nameValue{"Name1", "1"}, &nameValue{"Name1", "2"}},
			"name1", 3,
			WarcFields{&nameValue{"Name1", "3"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.initial.SetInt64(tt.fieldName, tt.fieldValue)
			assert.ElementsMatch(t, tt.want, tt.initial)
		})
	}
}

func TestWarcFields_SetTime(t *testing.T) {
	tests := []struct {
		name       string
		initial    WarcFields
		fieldName  string
		fieldValue time.Time
		want       WarcFields
	}{
		{"Set to empty",
			WarcFields{},
			"name1", time.Date(2021, 12, 25, 18, 30, 0, 20, time.UTC),
			WarcFields{&nameValue{"Name1", "2021-12-25T18:30:00Z"}}},
		{"Set new field",
			WarcFields{&nameValue{"Name1", "2021-12-25T18:30:00Z"}},
			"name2", time.Date(2022, 12, 25, 18, 30, 10, 20, time.UTC),
			WarcFields{&nameValue{"Name1", "2021-12-25T18:30:00Z"}, &nameValue{"Name2", "2022-12-25T18:30:10Z"}}},
		{"Set same field",
			WarcFields{&nameValue{"Name1", "2021-12-25T18:30:00Z"}},
			"name1", time.Date(2022, 12, 25, 18, 30, 10, 20, time.UTC),
			WarcFields{&nameValue{"Name1", "2022-12-25T18:30:10Z"}}},
		{"Set existing field with two values",
			WarcFields{&nameValue{"Name1", "1"}, &nameValue{"Name1", "2"}},
			"name1", time.Date(2023, 12, 25, 18, 30, 10, 20, time.UTC),
			WarcFields{&nameValue{"Name1", "2023-12-25T18:30:10Z"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.initial.SetTime(tt.fieldName, tt.fieldValue)
			assert.ElementsMatch(t, tt.want, tt.initial)
		})
	}
}

func TestWarcFields_Sort(t *testing.T) {
	tests := []struct {
		name    string
		initial WarcFields
		want    WarcFields
	}{
		{"Empty", WarcFields{}, WarcFields{}},
		{"One field",
			WarcFields{&nameValue{"name1", "value1"}},
			WarcFields{&nameValue{"name1", "value1"}}},
		{"Two fields, presorted",
			WarcFields{&nameValue{"name1", "value1"}, &nameValue{"name2", "value2"}},
			WarcFields{&nameValue{"name1", "value1"}, &nameValue{"name2", "value2"}}},
		{"Two fields, unsorted",
			WarcFields{&nameValue{"name2", "value2"}, &nameValue{"name1", "value1"}},
			WarcFields{&nameValue{"name1", "value1"}, &nameValue{"name2", "value2"}}},
		{"Two fields, same key",
			WarcFields{&nameValue{"name1", "value2"}, &nameValue{"name1", "value1"}},
			WarcFields{&nameValue{"name1", "value2"}, &nameValue{"name1", "value1"}}},
		{"Mix",
			WarcFields{&nameValue{"name3", "value4"}, &nameValue{"name1", "value2"},
				&nameValue{"name2", "value3"}, &nameValue{"name1", "value1"}},
			WarcFields{&nameValue{"name1", "value2"}, &nameValue{"name1", "value1"},
				&nameValue{"name2", "value3"}, &nameValue{"name3", "value4"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.initial.Sort()
			assert.Equal(t, tt.want, tt.initial)
		})
	}
}

func TestWarcFields_String(t *testing.T) {
	tests := []struct {
		name    string
		initial WarcFields
		want    string
	}{
		{"Empty", WarcFields{}, ""},
		{"One field",
			WarcFields{&nameValue{"name1", "value1"}},
			"name1: value1\r\n"},
		{"Two fields",
			WarcFields{&nameValue{"name1", "value1"}, &nameValue{"name2", "value2"}},
			"name1: value1\r\nname2: value2\r\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.initial.String())
		})
	}
}

func TestWarcFields_Write(t *testing.T) {
	tests := []struct {
		name             string
		initial          WarcFields
		wantW            string
		wantBytesWritten int64
		wantErr          bool
	}{
		{"Empty", WarcFields{}, "", 0, false},
		{"One field",
			WarcFields{&nameValue{"name1", "value1"}},
			"name1: value1\r\n", 15, false},
		{"Two fields",
			WarcFields{&nameValue{"name1", "value1"}, &nameValue{"name2", "value2"}},
			"name1: value1\r\nname2: value2\r\n", 30, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			gotBytesWritten, err := tt.initial.Write(w)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.wantW, w.String())
			assert.Equal(t, tt.wantBytesWritten, gotBytesWritten)
		})
	}
}

func TestWarcFields_AddTimeNano(t *testing.T) {
	tests := []struct {
		name         string
		fieldName    string
		fieldValue   time.Time
		wantContains string
	}{
		{
			name:         "time with nanoseconds",
			fieldName:    "WARC-Date",
			fieldValue:   time.Date(2020, time.January, 5, 10, 44, 25, 123456789, time.UTC),
			wantContains: "2020-01-05T10:44:25",
		},
		{
			name:         "time without nanoseconds",
			fieldName:    "WARC-Date",
			fieldValue:   time.Date(2020, time.January, 5, 10, 44, 25, 0, time.UTC),
			wantContains: "2020-01-05T10:44:25Z",
		},
		{
			name:         "non-UTC time",
			fieldName:    "WARC-Date",
			fieldValue:   time.Date(2020, time.January, 5, 10, 44, 25, 123456789, time.FixedZone("EST", -5*3600)),
			wantContains: "2020-01-05T15:44:25",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wf := WarcFields{}
			wf.AddTimeNano(tt.fieldName, tt.fieldValue)

			got := wf.Get(tt.fieldName)
			assert.Contains(t, got, tt.wantContains)

			// Should be valid RFC3339
			_, err := time.Parse(time.RFC3339Nano, got)
			require.NoError(t, err, "AddTimeNano() should produce valid RFC3339Nano format")
		})
	}
}

func TestWarcFields_clone(t *testing.T) {
	tests := []struct {
		name    string
		initial *WarcFields
	}{
		{"Empty", &WarcFields{}},
		{"One field", &WarcFields{&nameValue{"name1", "value1"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.initial.clone()
			assert.Equal(t, tt.initial, c)
			assert.NotSame(t, tt.initial, c)

			org := []*nameValue(*tt.initial)
			clone := []*nameValue(*c)
			for i, v := range org {
				assert.Equal(t, v, clone[i])
				assert.NotSame(t, v, clone[i])
			}
		})
	}
}

func TestWarcFields_AddId_EmptyValue(t *testing.T) {
	wf := WarcFields{}
	wf.AddId("name1", "")
	assert.Equal(t, 0, len(wf))
}

func TestWarcFields_SetId_EmptyValue(t *testing.T) {
	wf := WarcFields{&nameValue{"Name1", "existing"}}
	wf.SetId("name1", "")
	// Empty value should not set the field
	assert.Equal(t, 1, len(wf))
	assert.Equal(t, "existing", wf.Get("name1"))
}

func TestWarcFields_Write_Error(t *testing.T) {
	wf := WarcFields{
		&nameValue{"Name1", "value1"},
		&nameValue{"Name2", "value2"},
	}

	w := &failWriterAt{failAfter: 5, err: io.ErrClosedPipe}
	_, err := wf.Write(w)
	require.Error(t, err)
}

type failWriterAt struct {
	failAfter int
	written   int
	err       error
}

func (w *failWriterAt) Write(p []byte) (int, error) {
	if w.written+len(p) > w.failAfter {
		return 0, w.err
	}
	w.written += len(p)
	return len(p), nil
}
