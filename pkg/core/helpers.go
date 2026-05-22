package core

import (
	"crypto/rand"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const (
	BufferSize      = 64 * 1024 // 64K
	ConnDialTimeout = 5 * time.Second
	ConnDeadline    = 5 * time.Second
	ProbeTimeout    = 5 * time.Second
)

// Now90000 - timestamp for Video (clock rate = 90000 samples per second)
func Now90000() uint32 {
	return uint32(time.Duration(time.Now().UnixNano()) * 90000 / time.Second)
}

const symbols = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-_"

// RandString base10 - numbers, base16 - hex, base36 - digits+letters
// base64 - URL safe symbols, base0 - crypto random
func RandString(size, base byte) string {
	b := make([]byte, size)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	if base == 0 {
		return string(b)
	}
	for i := byte(0); i < size; i++ {
		b[i] = symbols[b[i]%base]
	}
	return string(b)
}

func Before(s, sep string) string {
	if i := strings.Index(s, sep); i > 0 {
		return s[:i]
	}
	return s
}

func Between(s, sub1, sub2 string) string {
	i := strings.Index(s, sub1)
	if i < 0 {
		return ""
	}
	s = s[i+len(sub1):]

	if i = strings.Index(s, sub2); i >= 0 {
		return s[:i]
	}

	return s
}

func Atoi(s string) (i int) {
	if s != "" {
		i, _ = strconv.Atoi(s)
	}
	return
}

// ParseByte - fast parsing string to byte function
func ParseByte(s string) (b byte) {
	for i, ch := range []byte(s) {
		ch -= '0'
		if ch > 9 {
			return 0
		}
		if i > 0 {
			b *= 10
		}
		b += ch
	}
	return
}

func Assert(ok bool) {
	if !ok {
		_, file, line, _ := runtime.Caller(1)
		panic(file + ":" + strconv.Itoa(line))
	}
}

func Caller() string {
	_, file, line, _ := runtime.Caller(1)
	return file + ":" + strconv.Itoa(line)
}

// StripUserinfo redacts the userinfo (username:password) portion of any
// rtsp:// or http:// URL embedded in the given string. The URL host,
// path, and query are preserved unchanged. Intended for sanitising
// streams.yaml content before it appears in logs or shared bug reports.
//
// Only the userinfo immediately preceding a scheme://host boundary is
// replaced, so a literal "@" inside a path or query (e.g.
// "rtsp://10.1.2.3:554/stream1@#video=copy") is left intact.
func StripUserinfo(s string) string {
	const placeholder = "***"
	var b strings.Builder
	b.Grow(len(s))
	for {
		i := strings.Index(s, "://")
		if i < 0 {
			b.WriteString(s)
			return b.String()
		}
		b.WriteString(s[:i+3])
		s = s[i+3:]
		// Find the next userinfo terminator before the host ends. The
		// host ends at '/', '?', '#', whitespace, or end-of-string.
		end := len(s)
		for j, ch := range s {
			if ch == '/' || ch == '?' || ch == '#' || ch == ' ' || ch == '\n' || ch == '\r' || ch == '\t' {
				end = j
				break
			}
		}
		if at := strings.LastIndex(s[:end], "@"); at >= 0 {
			b.WriteString(placeholder)
			b.WriteByte('@')
			s = s[at+1:]
		}
	}
}
