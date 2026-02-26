/*
 * Copyright 2021 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package internal

import (
	"strings"
	"testing"
)

func TestSprintt(t *testing.T) {
	tests := []struct {
		name   string
		format string
		params map[string]any
		want   string
	}{
		{
			name:   "single string parameter",
			format: "Hello %{name}s",
			params: map[string]any{"name": "world"},
			want:   "Hello world",
		},
		{
			name:   "single int parameter",
			format: "The answer is %{num}d",
			params: map[string]any{"num": 42},
			want:   "The answer is 42",
		},
		{
			name:   "multiple parameters",
			format: "Hello %{hello}s. The answer is %{num}d",
			params: map[string]any{"hello": "world", "num": 42},
			want:   "Hello world. The answer is 42",
		},
		{
			name:   "parameter used multiple times",
			format: "%{name}s loves %{name}s",
			params: map[string]any{"name": "Go"},
			want:   "Go loves Go",
		},
		{
			name:   "no parameters",
			format: "Hello world",
			params: map[string]any{},
			want:   "Hello world",
		},
		{
			name:   "empty format",
			format: "",
			params: map[string]any{"name": "test"},
			want:   "",
		},
		{
			name:   "unused parameters",
			format: "Hello %{name}s",
			params: map[string]any{"name": "world", "unused": 123},
			want:   "Hello world",
		},
		{
			name:   "float parameter",
			format: "Pi is approximately %{pi}f",
			params: map[string]any{"pi": 3.14159},
			want:   "Pi is approximately 3.141590",
		},
		{
			name:   "mixed types",
			format: "%{str}s has %{count}d items costing $%{price}f",
			params: map[string]any{"str": "Cart", "count": 5, "price": 19.99},
			want:   "Cart has 5 items costing $19.990000",
		},
		{
			name:   "width specifier before placeholder",
			format: "Number: %04{num}d",
			params: map[string]any{"num": 42},
			want:   "Number: 0042",
		},
		{
			name:   "special characters in value",
			format: "Message: %{msg}s",
			params: map[string]any{"msg": "Hello\nWorld"},
			want:   "Message: Hello\nWorld",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Sprintt(tt.format, tt.params)
			if got != tt.want {
				t.Errorf("Sprintt() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSprintt_EdgeCases(t *testing.T) {
	t.Run("nil params map", func(t *testing.T) {
		result := Sprintt("Hello world", nil)
		if result != "Hello world" {
			t.Errorf("Expected 'Hello world', got %q", result)
		}
	})

	t.Run("parameter name with special chars", func(t *testing.T) {
		// Parameter names with underscores, numbers
		result := Sprintt("Value: %{my_var_123}s", map[string]any{"my_var_123": "test"})
		if result != "Value: test" {
			t.Errorf("Expected 'Value: test', got %q", result)
		}
	})

	t.Run("curly braces without percent", func(t *testing.T) {
		// {name} without % should still be replaced because the function
		// just replaces {name} unconditionally, but without % it won't format properly
		result := Sprintt("Hello {name}", map[string]any{"name": "world"})
		// After replacement: "Hello [1]" then fmt.Sprintf adds the extra value
		expected := "Hello [1]%!(EXTRA string=world)"
		if result != expected {
			t.Errorf("Expected %q, got %q", expected, result)
		}
	})
}

func TestSprintt_ComplexPatterns(t *testing.T) {
	tests := []struct {
		name   string
		format string
		params map[string]any
		want   string
	}{
		{
			name:   "adjacent parameters",
			format: "%{first}s%{second}s",
			params: map[string]any{"first": "Hello", "second": "World"},
			want:   "HelloWorld",
		},
		{
			name:   "parameter at start and end",
			format: "%{start}s middle %{end}s",
			params: map[string]any{"start": "BEGIN", "end": "END"},
			want:   "BEGIN middle END",
		},
		{
			name:   "same parameter multiple times",
			format: "%{var}s and %{var}s again",
			params: map[string]any{"var": "test"},
			want:   "test and test again",
		},
		{
			name:   "numeric parameter types",
			format: "int:%{i}d float:%{f}f string:%{s}s",
			params: map[string]any{"i": 42, "f": 3.14, "s": "text"},
			want:   "int:42 float:3.140000 string:text",
		},
		{
			name:   "boolean parameter",
			format: "Value: %{flag}t",
			params: map[string]any{"flag": true},
			want:   "Value: true",
		},
		{
			name:   "empty parameter value",
			format: "Before %{empty}s After",
			params: map[string]any{"empty": ""},
			want:   "Before  After",
		},
		{
			name:   "parameter with underscores",
			format: "%{my_var_name}s",
			params: map[string]any{"my_var_name": "value"},
			want:   "value",
		},
		{
			name:   "parameter with numbers",
			format: "%{var123}s",
			params: map[string]any{"var123": "value"},
			want:   "value",
		},
		{
			name:   "many parameters",
			format: "%{a}s %{b}s %{c}s %{d}s %{e}s",
			params: map[string]any{"a": "1", "b": "2", "c": "3", "d": "4", "e": "5"},
			want:   "1 2 3 4 5",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Sprintt(tt.format, tt.params)
			if got != tt.want {
				t.Errorf("Sprintt() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSprintt_AdditionalEdgeCases(t *testing.T) {
	tests := []struct {
		name   string
		format string
		params map[string]any
		want   string
	}{
		{
			name:   "nil params map",
			format: "No parameters here",
			params: nil,
			want:   "No parameters here",
		},
		{
			name:   "empty format string",
			format: "",
			params: map[string]any{"key": "value"},
			want:   "",
		},
		{
			name:   "empty params map",
			format: "No %{params}s here",
			params: map[string]any{},
			want:   "No %!{(MISSING)params}s here",
		},
		{
			name:   "format with no parameters",
			format: "Just a plain string",
			params: map[string]any{"unused": "value"},
			want:   "Just a plain string",
		},
		{
			name:   "parameter name not in map",
			format: "Hello %{missing}s",
			params: map[string]any{"other": "value"},
			want:   "Hello %!{(MISSING)missing}s",
		},
		{
			name:   "parameter with wrong type",
			format: "Number: %{num}d",
			params: map[string]any{"num": "not a number"},
			want:   "Number: %!d(string=not a number)",
		},
		{
			name:   "nil value in params",
			format: "Value: %{nil}v",
			params: map[string]any{"nil": nil},
			want:   "Value: <nil>",
		},
		{
			name:   "format specifier before placeholder",
			format: "Padded: %04{num}d",
			params: map[string]any{"num": 42},
			want:   "Padded: 0042",
		},
		{
			name:   "format specifier with precision",
			format: "Float: %.2{val}f",
			params: map[string]any{"val": 3.14159},
			want:   "Float: 3.14",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Sprintt(tt.format, tt.params)
			if got != tt.want {
				t.Errorf("Sprintt() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSprintt_SpecialCharacters(t *testing.T) {
	tests := []struct {
		name   string
		format string
		params map[string]any
		want   string
	}{
		{
			name:   "newline in value",
			format: "Text: %{text}s",
			params: map[string]any{"text": "line1\nline2"},
			want:   "Text: line1\nline2",
		},
		{
			name:   "tab in value",
			format: "Text: %{text}s",
			params: map[string]any{"text": "col1\tcol2"},
			want:   "Text: col1\tcol2",
		},
		{
			name:   "percent in value",
			format: "Percent: %{pct}s",
			params: map[string]any{"pct": "100%"},
			want:   "Percent: 100%",
		},
		{
			name:   "curly braces in value",
			format: "Braces: %{val}s",
			params: map[string]any{"val": "{test}"},
			want:   "Braces: {test}",
		},
		{
			name:   "unicode in value",
			format: "Unicode: %{text}s",
			params: map[string]any{"text": "Hello 世界"},
			want:   "Unicode: Hello 世界",
		},
		{
			name:   "backslash in value",
			format: "Path: %{path}s",
			params: map[string]any{"path": "C:\\Users\\test"},
			want:   "Path: C:\\Users\\test",
		},
		{
			name:   "quotes in value",
			format: "Quote: %{text}s",
			params: map[string]any{"text": `"quoted"`},
			want:   `Quote: "quoted"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Sprintt(tt.format, tt.params)
			if got != tt.want {
				t.Errorf("Sprintt() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSprintt_ComplexTypes(t *testing.T) {
	tests := []struct {
		name   string
		format string
		params map[string]any
		want   string
	}{
		{
			name:   "struct value",
			format: "Struct: %{s}v",
			params: map[string]any{"s": struct{ Name string }{"test"}},
			want:   "Struct: {test}",
		},
		{
			name:   "slice value",
			format: "Slice: %{arr}v",
			params: map[string]any{"arr": []int{1, 2, 3}},
			want:   "Slice: [1 2 3]",
		},
		{
			name:   "map value",
			format: "Map: %{m}v",
			params: map[string]any{"m": map[string]int{"a": 1}},
			want:   "Map: map[a:1]",
		},
		{
			name:   "pointer value",
			format: "Pointer: %{p}p",
			params: map[string]any{"p": func() *int { i := 42; return &i }()},
			want:   "Pointer: ", // Will contain address
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Sprintt(tt.format, tt.params)
			// For pointer test, just check it starts with expected prefix
			if tt.name == "pointer value" {
				if !strings.HasPrefix(got, tt.want) {
					t.Errorf("Sprintt() = %q, want prefix %q", got, tt.want)
				}
			} else {
				if got != tt.want {
					t.Errorf("Sprintt() = %q, want %q", got, tt.want)
				}
			}
		})
	}
}

func TestSprintt_LargeFormat(t *testing.T) {
	// Test with many replacements
	format := ""
	params := make(map[string]any)

	for i := 0; i < 100; i++ {
		key := "var" + string(rune('0'+i%10))
		format += "%{" + key + "}s "
		params[key] = "value"
	}

	result := Sprintt(format, params)

	// Should not panic and should produce a result
	if result == "" {
		t.Error("Sprintt() with large format should not return empty string")
	}
}

func TestSprintt_ParameterNameEdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		format      string
		params      map[string]any
		description string
	}{
		{
			name:        "single character name",
			format:      "%{x}s",
			params:      map[string]any{"x": "test"},
			description: "single char param names should work",
		},
		{
			name:        "long parameter name",
			format:      "%{very_long_parameter_name_with_underscores}s",
			params:      map[string]any{"very_long_parameter_name_with_underscores": "test"},
			description: "long param names should work",
		},
		{
			name:        "numeric start",
			format:      "%{1var}s",
			params:      map[string]any{"1var": "test"},
			description: "param names starting with number should work",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Sprintt() panicked with %s: %v", tt.description, r)
				}
			}()

			result := Sprintt(tt.format, tt.params)
			if result == "" {
				t.Errorf("Sprintt() returned empty string for %s", tt.description)
			}
		})
	}
}

func TestSprintt_ConsistentBehavior(t *testing.T) {
	// Test that multiple calls with same inputs produce same output
	format := "Hello %{name}s, you are %{age}d years old"
	params := map[string]any{"name": "Alice", "age": 30}

	result1 := Sprintt(format, params)
	result2 := Sprintt(format, params)

	if result1 != result2 {
		t.Errorf("Sprintt() not consistent: first call = %q, second call = %q", result1, result2)
	}
}

func TestSprintt_OrderIndependent(t *testing.T) {
	// Test that parameter order in format doesn't affect unused params
	format1 := "A: %{a}s, B: %{b}s"
	format2 := "B: %{b}s, A: %{a}s"
	params := map[string]any{"a": "first", "b": "second", "c": "unused"}

	result1 := Sprintt(format1, params)
	result2 := Sprintt(format2, params)

	expected1 := "A: first, B: second"
	expected2 := "B: second, A: first"

	if result1 != expected1 {
		t.Errorf("Sprintt() format1 = %q, want %q", result1, expected1)
	}
	if result2 != expected2 {
		t.Errorf("Sprintt() format2 = %q, want %q", result2, expected2)
	}
}

func TestSprintt_NoReplacement(t *testing.T) {
	// Test strings that look like they might have parameters but don't
	tests := []struct {
		name   string
		format string
		params map[string]any
		want   string
	}{
		{
			name:   "just curly braces",
			format: "Text with {braces} but no percent",
			params: map[string]any{"braces": "value"},
			want:   "Text with [1] but no percent%!(EXTRA string=value)",
		},
		{
			name:   "percent but no braces",
			format: "Text with % percent",
			params: map[string]any{},
			want:   "Text with %!p(MISSING)ercent",
		},
		{
			name:   "incomplete pattern",
			format: "Text with %{incomplete",
			params: map[string]any{"incomplete": "value"},
			want:   "Text with %!{(MISSING)incomplete",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Sprintt(tt.format, tt.params)
			if got != tt.want {
				t.Errorf("Sprintt() = %q, want %q", got, tt.want)
			}
		})
	}
}
