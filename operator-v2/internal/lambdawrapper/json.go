package lambdawrapper

import (
	"bytes"
	"encoding/json"
	"io"
	"reflect"
	"strings"
	"unicode/utf8"
)

// inspectJSON rejects duplicate keys at every level before typed decoding.
// Byte, token and nesting limits bound both validation and subsequent decoding.
func inspectJSON(data []byte, max int) error {
	if len(data) > max || !utf8.Valid(data) {
		return fail("WrapperJSONLimit")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	count := 0
	var value func(int) error
	value = func(depth int) error {
		count++
		if depth > 16 || count > 8192 {
			return fail("WrapperJSONLimit")
		}
		t, err := d.Token()
		if err != nil {
			return fail("WrapperInvalidJSON")
		}
		if delim, ok := t.(json.Delim); ok {
			switch delim {
			case '{':
				keys := map[string]bool{}
				for d.More() {
					count++
					if count > 8192 {
						return fail("WrapperJSONLimit")
					}
					k, err := d.Token()
					key, ok := k.(string)
					if err != nil || !ok || keys[key] {
						return fail("WrapperInvalidJSON")
					}
					keys[key] = true
					if err = value(depth + 1); err != nil {
						return err
					}
				}
				t, err = d.Token()
				if err != nil || t != json.Delim('}') {
					return fail("WrapperInvalidJSON")
				}
			case '[':
				for d.More() {
					if err = value(depth + 1); err != nil {
						return err
					}
				}
				t, err = d.Token()
				if err != nil || t != json.Delim(']') {
					return fail("WrapperInvalidJSON")
				}
			default:
				return fail("WrapperInvalidJSON")
			}
		}
		return nil
	}
	if err := value(0); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return fail("WrapperInvalidJSON")
	}
	return nil
}

func decodeJSON(data []byte, max int, out any) error {
	if err := inspectJSON(data, max); err != nil {
		return err
	}
	if err := exactFields(data, reflect.TypeOf(out)); err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return fail("WrapperInvalidJSON")
	}
	return nil
}

// encoding/json otherwise accepts case-insensitive aliases for struct fields.
func exactFields(data []byte, t reflect.Type) error {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == reflect.TypeFor[json.RawMessage]() {
		return nil
	}
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return nil
	}
	switch t.Kind() {
	case reflect.Struct:
		var object map[string]json.RawMessage
		if json.Unmarshal(data, &object) != nil {
			return fail("WrapperInvalidJSON")
		}
		fields := map[string]reflect.Type{}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			name := strings.Split(f.Tag.Get("json"), ",")[0]
			if name == "" {
				name = f.Name
			}
			fields[name] = f.Type
		}
		for key, value := range object {
			ft, ok := fields[key]
			if !ok {
				return fail("WrapperInvalidJSON")
			}
			if err := exactFields(value, ft); err != nil {
				return err
			}
		}
	case reflect.Slice:
		// []byte is intentionally not used in the bootstrap wire schema.
		var items []json.RawMessage
		if json.Unmarshal(data, &items) != nil {
			return fail("WrapperInvalidJSON")
		}
		for _, item := range items {
			if err := exactFields(item, t.Elem()); err != nil {
				return err
			}
		}
	}
	return nil
}
