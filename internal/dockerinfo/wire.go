package dockerinfo

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// This connection is owned by one operation. There is no transport pool,
// redirect handler, retry loop, proxy, decompressor or second dial.
type wire struct {
	conn        net.Conn
	reader      *bufio.Reader
	api, server string
}

func newWire(conn net.Conn) *wire { return &wire{conn: conn, reader: bufio.NewReaderSize(conn, 4096)} }

func readLine(r *bufio.Reader, limit int) ([]byte, error) {
	line := make([]byte, 0, 128)
	for {
		part, err := r.ReadSlice('\n')
		if len(part) > limit-len(line) {
			return nil, ErrBounds
		}
		line = append(line, part...)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if !bytes.HasSuffix(line, []byte("\r\n")) {
			return nil, ErrProtocol
		}
		return line, nil
	}
}

func (w *wire) get(ctx context.Context, path string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://docker/v"+APIVersion+path, nil)
	if err != nil {
		return nil, ErrProtocol
	}
	req.Header.Set("Accept", "application/json")
	if err := req.Write(w.conn); err != nil {
		return nil, errors.Join(ErrProtocol, err)
	}
	var header []byte
	seenHeaders := map[string]bool{}
	for {
		line, err := readLine(w.reader, MaxHeaderBytes-len(header))
		if err != nil {
			return nil, errors.Join(ErrProtocol, err)
		}
		if len(header) != 0 && !bytes.Equal(line, []byte("\r\n")) {
			key, _, ok := strings.Cut(string(line), ":")
			key = strings.ToLower(key)
			if !ok || seenHeaders[key] {
				return nil, ErrProtocol
			}
			seenHeaders[key] = true
		}
		header = append(header, line...)
		if bytes.Equal(line, []byte("\r\n")) {
			break
		}
	}
	if seenHeaders["content-length"] && seenHeaders["transfer-encoding"] {
		return nil, ErrProtocol
	}
	resp, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(header)), req)
	if err != nil {
		return nil, errors.Join(ErrProtocol, err)
	}
	// The parsed Body points only at the bounded header buffer; live body reads
	// below use explicit framing and bounds on the original held connection.
	if resp.StatusCode != http.StatusOK || resp.ProtoMajor != 1 || resp.ProtoMinor != 1 || resp.Close || len(resp.Header.Values("Content-Encoding")) != 0 || len(resp.Trailer) != 0 {
		return nil, ErrProtocol
	}
	for _, key := range []string{"API-Version", "OSType", "Server", "Content-Type"} {
		if len(resp.Header.Values(key)) != 1 {
			return nil, ErrProtocol
		}
	}
	media, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	api, server := resp.Header.Get("API-Version"), resp.Header.Get("Server")
	if err != nil || media != "application/json" || !versionAtLeast44(api) || resp.Header.Get("OSType") != "linux" || !safeText(server, 256) || !strings.HasPrefix(server, "Docker/") || !strings.HasSuffix(server, " (linux)") {
		return nil, ErrProtocol
	}
	if w.api == "" {
		w.api, w.server = api, server
	} else if w.api != api || w.server != server {
		return nil, ErrDaemonChanged
	}
	var body []byte
	if len(resp.TransferEncoding) == 0 {
		if resp.ContentLength < 0 || resp.ContentLength > MaxBodyBytes {
			return nil, ErrBounds
		}
		body = make([]byte, int(resp.ContentLength))
		if _, err := io.ReadFull(w.reader, body); err != nil {
			return nil, errors.Join(ErrProtocol, err)
		}
	} else {
		if len(resp.TransferEncoding) != 1 || resp.TransferEncoding[0] != "chunked" || len(resp.Header.Values("Content-Length")) != 0 {
			return nil, ErrProtocol
		}
		for chunks := 0; ; chunks++ {
			if chunks >= 1024 {
				return nil, ErrBounds
			}
			line, err := readLine(w.reader, 1024)
			if err != nil {
				return nil, errors.Join(ErrProtocol, err)
			}
			num := string(line[:len(line)-2])
			if num == "" || strings.ContainsAny(num, ";+- \t") {
				return nil, ErrProtocol
			}
			n, err := strconv.ParseUint(num, 16, 64)
			if err != nil {
				return nil, ErrProtocol
			}
			if n > uint64(MaxBodyBytes-len(body)) {
				return nil, ErrBounds
			}
			if n == 0 {
				end, err := readLine(w.reader, MaxHeaderBytes)
				if err != nil || !bytes.Equal(end, []byte("\r\n")) {
					return nil, errors.Join(ErrProtocol, err)
				}
				break
			}
			start := len(body)
			body = append(body, make([]byte, int(n))...)
			if _, err := io.ReadFull(w.reader, body[start:]); err != nil {
				return nil, errors.Join(ErrProtocol, err)
			}
			var end [2]byte
			if _, err := io.ReadFull(w.reader, end[:]); err != nil || end != [2]byte{'\r', '\n'} {
				return nil, errors.Join(ErrProtocol, err)
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := strictJSON(body); err != nil {
		return nil, err
	}
	return body, nil
}

type daemonInfo struct{ ID, OSType, ServerVersion string }

// Select exact schema keys before typed decoding. encoding/json's permissive
// case-insensitive struct matching must not let an unrelated key replace one
// of the explicitly projected fields.
func projectedObject(b []byte, names ...string) ([]byte, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(b, &fields); err != nil || fields == nil {
		return nil, ErrProtocol
	}
	selected := make(map[string]json.RawMessage, len(names))
	for _, name := range names {
		if value, ok := fields[name]; ok {
			selected[name] = value
		}
	}
	return json.Marshal(selected)
}

func (w *wire) info(ctx context.Context) (daemonInfo, error) {
	b, err := w.get(ctx, "/info")
	if err != nil {
		return daemonInfo{}, err
	}
	b, err = projectedObject(b, "ID", "OSType", "ServerVersion")
	if err != nil {
		return daemonInfo{}, err
	}
	var info daemonInfo
	if err := json.Unmarshal(b, &info); err != nil {
		return daemonInfo{}, ErrProtocol
	}
	major, _, ok := strings.Cut(info.ServerVersion, ".")
	n, err := strconv.Atoi(major)
	if info.ID == "" || strings.TrimSpace(info.ID) != info.ID || !safeText(info.ID, 256) || info.OSType != "linux" || !ok || err != nil || n < 25 || !safeText(info.ServerVersion, 128) || w.server != "Docker/"+info.ServerVersion+" (linux)" {
		return daemonInfo{}, ErrProtocol
	}
	return info, nil
}

func createdAt(seconds int64) (time.Time, error) {
	if seconds < 0 || seconds > 253402300799 {
		return time.Time{}, ErrProtocol
	}
	return time.Unix(seconds, 0).UTC(), nil
}

func boundedNames(names []string) ([]string, error) {
	if len(names) > 32 {
		return nil, ErrBounds
	}
	result := make([]string, 0, len(names))
	for _, name := range names {
		if name == "" || !safeText(name, 1024) {
			return nil, ErrProtocol
		}
		result = append(result, name)
	}
	return result, nil
}

func (w *wire) images(ctx context.Context) ([]Image, error) {
	b, err := w.get(ctx, "/images/json?all=true")
	if err != nil {
		return nil, err
	}
	var rows []json.RawMessage
	if err := json.Unmarshal(b, &rows); err != nil || rows == nil {
		return nil, ErrProtocol
	}
	if len(rows) > MaxObjects {
		return nil, ErrBounds
	}
	result, seen := make([]Image, 0, len(rows)), make(map[string]bool, len(rows))
	for _, raw := range rows {
		selected, err := projectedObject(raw, "Id", "RepoTags", "Created")
		if err != nil {
			return nil, err
		}
		var row struct {
			ID       string `json:"Id"`
			RepoTags []string
			Created  *int64
		}
		if err := json.Unmarshal(selected, &row); err != nil {
			return nil, ErrProtocol
		}
		if !validDigest(row.ID, true) || seen[row.ID] || row.Created == nil {
			return nil, ErrProtocol
		}
		seen[row.ID] = true
		tags, err := boundedNames(row.RepoTags)
		if err != nil {
			return nil, err
		}
		created, err := createdAt(*row.Created)
		if err != nil {
			return nil, err
		}
		result = append(result, Image{ID: row.ID, Tags: tags, CreatedAt: created})
	}
	return result, nil
}

func (w *wire) containers(ctx context.Context) ([]Container, error) {
	b, err := w.get(ctx, "/containers/json?all=true&size=false&limit=129")
	if err != nil {
		return nil, err
	}
	var rows []json.RawMessage
	if err := json.Unmarshal(b, &rows); err != nil || rows == nil {
		return nil, ErrProtocol
	}
	if len(rows) > MaxObjects {
		return nil, ErrBounds
	}
	result, seen := make([]Container, 0, len(rows)), make(map[string]bool, len(rows))
	for _, raw := range rows {
		selected, err := projectedObject(raw, "Id", "Names", "Created", "State")
		if err != nil {
			return nil, err
		}
		var row struct {
			ID      string `json:"Id"`
			Names   []string
			Created *int64
			State   string
		}
		if err := json.Unmarshal(selected, &row); err != nil {
			return nil, ErrProtocol
		}
		if !validDigest(row.ID, false) || seen[row.ID] || row.Created == nil {
			return nil, ErrProtocol
		}
		seen[row.ID] = true
		switch row.State {
		case "created", "restarting", "running", "removing", "paused", "exited", "dead":
		default:
			return nil, ErrProtocol
		}
		names, err := boundedNames(row.Names)
		if err != nil {
			return nil, err
		}
		created, err := createdAt(*row.Created)
		if err != nil {
			return nil, err
		}
		result = append(result, Container{ID: row.ID, Names: names, CreatedAt: created, State: row.State})
	}
	return result, nil
}
