package service

import (
	"sort"
	"strings"
)

func renderSystemd(descriptor Descriptor) string {
	var content strings.Builder
	content.WriteString("[Unit]\nDescription=Saga - Rydd idle worker\n\n[Service]\nType=exec\n")
	content.WriteString("ExecStart=")
	for index, argument := range descriptor.Argv {
		if index == 0 {
			// Prefix recognition follows whole-item unquoting. Colon disables
			// environment-variable substitution for this entire command line.
			argument = ":" + argument
		} else {
			content.WriteByte(' ')
		}
		content.WriteString(systemdLiteral(argument))
	}
	content.WriteByte('\n')
	keys := make([]string, 0, len(descriptor.Env))
	for key := range descriptor.Env {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		content.WriteString("Environment=")
		content.WriteString(systemdLiteral(key + "=" + descriptor.Env[key]))
		content.WriteByte('\n')
	}
	// These manager scheduling hints supplement internal pacing; they are not
	// cgroup quotas or evidence of effective installed priority.
	content.WriteString("Restart=on-failure\nRestartSec=30s\nTimeoutStopSec=10s\nNice=10\nIOSchedulingClass=idle\n\n[Install]\nWantedBy=default.target\n")
	return content.String()
}

func systemdLiteral(value string) string {
	var escaped strings.Builder
	escaped.WriteByte('"')
	for _, r := range value {
		switch r {
		case '\\', '"':
			escaped.WriteByte('\\')
			escaped.WriteRune(r)
		case '%':
			escaped.WriteString("%%")
		default:
			escaped.WriteRune(r)
		}
	}
	escaped.WriteByte('"')
	return escaped.String()
}
