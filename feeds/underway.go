package feeds

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/ctberthiaume/cruisemic/parse"
)

type Underway struct {
	i        int // index of next item to emit
	data     []underwayRecord
	conn     net.Conn
	warnings []Warning
}

func NewUnderway(file string, host string, port uint, throttleSec int64) (u *Underway, err error) {
	u = &Underway{i: -1}
	u.data = []underwayRecord{}
	u.conn, err = net.Dial("udp", fmt.Sprintf("%v:%d", host, port))
	if err != nil {
		return u, fmt.Errorf("underway: %v", err)
	}

	parserFact, ok := parse.ParserRegistry["Kilo Moana"]
	if !ok {
		panic(fmt.Errorf("invalid parser choice"))
	}
	throttle := time.Duration(throttleSec * int64(time.Second))
	parser := parserFact("", throttle) // rate limit to one record type per minute
	f, err := os.Open(file)
	if err != nil {
		return
	}
	defer f.Close()
	var r io.Reader
	if strings.HasSuffix(file, ".gz") {
		r, err = gzip.NewReader(f)
		if err != nil {
			return u, fmt.Errorf("underway: %v", err)
		}
	} else {
		r = bufio.NewReader(f)
	}
	i := 0
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		i++
		b := scanner.Bytes()
		// Remove unwanted ASCII characters
		n := parse.Whitelist(b, len(b))
		line := string(b[:n])

		d, err := parser.ParseLine(line)
		if err != nil {
			newErr := fmt.Errorf("underway: line %d: %v", i, err)
			u.warnings = append(u.warnings, Warning{err: newErr})
		} else if d.OK() {
			u.data = append(u.data, underwayRecord{time: d.Time, data: line})
		}
	}
	if err := scanner.Err(); err != nil {
		return u, fmt.Errorf("underway: %v", err)
	}

	// Sort by time, ascending
	sort.SliceStable(u.data, func(i, j int) bool {
		return u.data[i].time.Before(u.data[j].time)
	})

	// Coalesce records with identical times to the second
	if len(u.data) > 0 {
		newdata := []underwayRecord{}
		t := u.data[0].time.Truncate(time.Second)
		lines := []string{u.data[0].data}
		for i := 1; i < len(u.data); i++ {
			if u.data[i].time.Truncate(time.Second).Equal(t) {
				lines = append(lines, u.data[i].data)
			} else {
				newdata = append(newdata, underwayRecord{time: t, data: strings.Join(lines, "\n")})
				lines = lines[:0]
				lines = append(lines, u.data[i].data)
				t = u.data[i].time.Truncate(time.Second)
			}
		}
		u.data = append(newdata, underwayRecord{time: t, data: strings.Join(lines, "\n")})
	}

	return u, nil
}

func (u *Underway) Close() (err error) {
	if u.conn != nil {
		if err = u.conn.Close(); err != nil {
			return fmt.Errorf("underway: %v", err)
		}
	}

	return
}

func (u *Underway) Earliest() (t time.Time) {
	if len(u.data) > 0 {
		t = u.data[0].time
	}
	return
}

func (u *Underway) Emit() (err error) {
	if u.i < 0 {
		return
	}
	if _, err = u.conn.Write([]byte(u.data[u.i].data + "\n")); err != nil {
		return fmt.Errorf("underway: %v", err)
	}
	return
}

func (u *Underway) Time() (t time.Time) {
	if u.i >= 0 && len(u.data) > 0 {
		t = u.data[u.i].time
	}
	return
}

func (u *Underway) Next() bool {
	if u.i+1 < len(u.data) {
		u.i++
		return true
	}
	return false
}

func (u *Underway) Warnings() []Warning {
	return u.warnings
}

func (u *Underway) Name() string {
	return "underway"
}

func (u *Underway) Len() int {
	return len(u.data)
}

type underwayRecord struct {
	time time.Time
	data string
}

func (ur underwayRecord) String() string {
	return fmt.Sprintf("%v %s", ur.time, ur.data)
}
