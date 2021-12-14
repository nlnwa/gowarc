package gowarc

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.initial.GetInt(tt.fieldName)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.want, got)
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.initial.GetInt64(tt.fieldName)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.want, got)
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.initial.GetTime(tt.fieldName)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tt.want, got)
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
