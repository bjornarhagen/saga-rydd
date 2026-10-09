package service

import (
	"bytes"
	"encoding/xml"
	"sort"
)

func renderLaunchd(descriptor Descriptor) (string, error) {
	var content bytes.Buffer
	content.WriteString(xml.Header)
	content.WriteString("<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n")
	encoder := xml.NewEncoder(&content)
	encoder.Indent("", "  ")
	start := func(name string) error {
		return encoder.EncodeToken(xml.StartElement{Name: xml.Name{Local: name}})
	}
	end := func(name string) error {
		return encoder.EncodeToken(xml.EndElement{Name: xml.Name{Local: name}})
	}
	text := func(name, value string) error {
		return encoder.EncodeElement(value, xml.StartElement{Name: xml.Name{Local: name}})
	}
	boolean := func(value bool) error {
		name := "false"
		if value {
			name = "true"
		}
		if err := start(name); err != nil {
			return err
		}
		return end(name)
	}
	if err := encoder.EncodeToken(xml.StartElement{Name: xml.Name{Local: "plist"}, Attr: []xml.Attr{{Name: xml.Name{Local: "version"}, Value: "1.0"}}}); err != nil {
		return "", err
	}
	if err := start("dict"); err != nil {
		return "", err
	}
	for _, pair := range [][2]string{{"Label", descriptor.Label}, {"ProcessType", "Background"}} {
		if err := text("key", pair[0]); err != nil {
			return "", err
		}
		if err := text("string", pair[1]); err != nil {
			return "", err
		}
	}
	if err := text("key", "ProgramArguments"); err != nil {
		return "", err
	}
	if err := start("array"); err != nil {
		return "", err
	}
	for _, argument := range descriptor.Argv {
		if err := text("string", argument); err != nil {
			return "", err
		}
	}
	if err := end("array"); err != nil {
		return "", err
	}
	if err := text("key", "EnvironmentVariables"); err != nil {
		return "", err
	}
	if err := start("dict"); err != nil {
		return "", err
	}
	keys := make([]string, 0, len(descriptor.Env))
	for key := range descriptor.Env {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := text("key", key); err != nil {
			return "", err
		}
		if err := text("string", descriptor.Env[key]); err != nil {
			return "", err
		}
	}
	if err := end("dict"); err != nil {
		return "", err
	}
	if err := text("key", "LowPriorityIO"); err != nil {
		return "", err
	}
	if err := boolean(true); err != nil {
		return "", err
	}
	if err := text("key", "KeepAlive"); err != nil {
		return "", err
	}
	if err := start("dict"); err != nil {
		return "", err
	}
	if err := text("key", "SuccessfulExit"); err != nil {
		return "", err
	}
	if err := boolean(false); err != nil {
		return "", err
	}
	if err := end("dict"); err != nil {
		return "", err
	}
	for _, pair := range [][2]string{{"ExitTimeOut", "10"}, {"ThrottleInterval", "30"}} {
		if err := text("key", pair[0]); err != nil {
			return "", err
		}
		if err := text("integer", pair[1]); err != nil {
			return "", err
		}
	}
	if err := end("dict"); err != nil {
		return "", err
	}
	if err := end("plist"); err != nil {
		return "", err
	}
	if err := encoder.Flush(); err != nil {
		return "", err
	}
	content.WriteByte('\n')
	return content.String(), nil
}
