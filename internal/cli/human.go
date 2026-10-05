package cli

import (
	"fmt"
	"io"
	"os"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/bjornarhagen/saga-rydd/internal/state"
	"github.com/mattn/go-isatty"
	"golang.org/x/sys/unix"
)

func shellQuote(s string) string {
	if utf8.ValidString(s) && strings.IndexFunc(s, func(r rune) bool { return !unicode.IsPrint(r) }) < 0 {
		return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'"
	}
	// Bash and Zsh ANSI-C quoting preserves every byte (including trailing
	// newlines) without printing controls to the terminal or executing path text.
	var quoted strings.Builder
	quoted.WriteString("$'")
	for i := 0; i < len(s); i++ {
		fmt.Fprintf(&quoted, "\\%03o", s[i])
	}
	quoted.WriteByte('\'')
	return quoted.String()
}

// Human formatting stays outside the versioned JSON contract. Paths, IDs and
// commands are printed separately: wrapping or truncating them can change meaning.
func outputWidth(out io.Writer) int {
	if f, ok := out.(*os.File); ok {
		if size, err := unix.IoctlGetWinsize(int(f.Fd()), unix.TIOCGWINSZ); err == nil && size.Col > 0 {
			return min(int(size.Col), 100)
		}
	}
	return 78
}

// Use a conservative upper bound for non-ASCII prose without adding a Unicode
// layout dependency. Combining marks occupy no extra columns; other non-ASCII
// characters reserve two. Full quoted paths are never split to fit this bound.
func proseWidth(s string) int {
	n := 0
	for _, r := range s {
		switch {
		case unicode.Is(unicode.Mn, r), unicode.Is(unicode.Me, r):
		case r > 127:
			n += 2
		default:
			n++
		}
	}
	return n
}

func printWrapped(out io.Writer, text, prefix string) {
	writeWrapped(out, text, prefix, strings.Repeat(" ", proseWidth(prefix)), outputWidth(out))
}

func writeWrapped(out io.Writer, text, prefix, continuation string, width int) {
	line := prefix
	hasWord := false
	for _, word := range strings.Fields(text) {
		if hasWord && proseWidth(line)+1+proseWidth(word) > width {
			fmt.Fprintln(out, line)
			line = continuation
			hasWord = false
		}
		if hasWord {
			line += " "
		}
		line += word
		hasWord = true
	}
	fmt.Fprintln(out, line)
}

func printField(out io.Writer, label string, value any) {
	writeField(out, label, fmt.Sprint(value), outputWidth(out))
}

func writeField(out io.Writer, label, value string, width int) {
	if width < 60 {
		fmt.Fprintf(out, "  %s:\n", label)
		writeWrapped(out, value, "    ", "    ", width)
		return
	}
	prefix := fmt.Sprintf("  %-25s ", label)
	writeWrapped(out, value, prefix, strings.Repeat(" ", len(prefix)), width)
}

func printResultBanner(out io.Writer, title string) {
	color := false
	if f, ok := out.(*os.File); ok {
		_, noColor := os.LookupEnv("NO_COLOR")
		color = !noColor && os.Getenv("TERM") != "dumb" && isatty.IsTerminal(f.Fd())
	}
	fmt.Fprintln(out)
	if color {
		fmt.Fprint(out, "\x1b[1;33m")
	}
	printWrapped(out, title, "")
	fmt.Fprintln(out, strings.Repeat("-", min(proseWidth(title), outputWidth(out))))
	if color {
		fmt.Fprint(out, "\x1b[0m")
	}
}

func directoryStatusLabel(status string) string {
	switch status {
	case "recorded_complete":
		return "Complete in saved scan"
	case "partial":
		return "Incomplete measurement"
	case "stale":
		return "Saved data may be outdated"
	default:
		return "Unknown"
	}
}

func savedBytes(n *int64) string {
	if n == nil {
		return "Unknown"
	}
	return humanBytes(*n)
}

func printSavedMeasurement(out io.Writer, r state.DirectoryReport) {
	printField(out, "File size", savedBytes(r.LogicalBytes))
	printField(out, "Allocated on disk", savedBytes(r.AllocatedBytes))
	printField(out, "Measurement", directoryStatusLabel(r.Status))
	if r.UnknownReason != "" {
		printWrapped(out, r.UnknownReason, "  ")
	}
	if r.Status == "partial" || r.Status == "stale" {
		printWrapped(out, "These sizes may overstate or understate the current folder size.", "  ")
	}
	if r.Truncated {
		printWrapped(out, "Entry limit reached; sizes cover only the measured portion.", "  ")
	}
}

func printFinding(out io.Writer, number int, f state.Finding) {
	fmt.Fprintf(out, "\n%d. %q\n", number, string(f.PathBytes))
	printSavedMeasurement(out, f.Measurement)
	printField(out, "Folder modified", f.DirectoryModifiedAt.Format("2006-01-02"))
	printField(out, "package.json modified", f.ManifestModifiedAt.Format("2006-01-02"))
	fmt.Fprintf(out, "  Reference: %s\n", f.ID)
}
