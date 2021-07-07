package feeds

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/seaflow-uw/seaflog"
)

type SeaLog struct {
	i        int // index of next item to emit
	data     []seaLogRecord
	outDir   string
	file     *os.File // current output file
	warnings []Warning
}

func NewSeaLog(file string, outDir string) (s *SeaLog, err error) {
	s = &SeaLog{i: -1}
	s.data = []seaLogRecord{}
	s.outDir = outDir

	r, err := os.Open(file)
	if err != nil {
		return s, fmt.Errorf("seaflowlog: %v", err)
	}
	defer r.Close()
	bufr := bufio.NewReader(r)

	sc := seaflog.NewEventScanner(bufr)
	for sc.Scan() {
		event := sc.Event()
		if event.Name != "unhandled" {
			s.data = append(s.data, seaLogRecord{time: event.Time, data: event.Line})
		} else {
			newErr := fmt.Errorf("seaflowlog: unhandled event at line %d: %s", event.LineNumber, event.Line)
			s.warnings = append(s.warnings, Warning{err: newErr})
		}
	}
	if err = sc.Err(); err != nil {
		return s, fmt.Errorf("seaflowlog: %v", err)
	}

	// Sort by time, ascending
	sort.SliceStable(s.data, func(i, j int) bool {
		return s.data[i].time.Before(s.data[j].time)
	})

	return s, nil
}

func (s *SeaLog) Close() (err error) {
	if s.file != nil {
		err = s.file.Close()
		s = nil
	}
	return
}

func (s *SeaLog) Earliest() (t time.Time) {
	if len(s.data) > 0 {
		t = s.data[0].time
	}
	return
}

func (s *SeaLog) Emit() (err error) {
	if s.i < 0 {
		return
	}
	rec := s.data[s.i]
	outDir := filepath.Join(s.outDir, "logs")
	if err = os.MkdirAll(outDir, os.ModePerm); err != nil {
		return fmt.Errorf("seaflowlog: %v", err)
	}
	outPath := filepath.Join(outDir, "SFlog.txt")
	if s.file == nil {
		if s.file, err = os.OpenFile(outPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModePerm); err != nil {
			return fmt.Errorf("seaflowlog: %v", err)
		}
	}
	s.file.WriteString(fmt.Sprintf("%s\r\n", rec))

	return
}

func (s *SeaLog) Time() (t time.Time) {
	if s.i >= 0 && len(s.data) > 0 {
		t = s.data[s.i].time
	}
	return
}

func (s *SeaLog) Next() bool {
	if s.i+1 < len(s.data) {
		s.i++
		return true
	}
	return false
}

func (s *SeaLog) Warnings() []Warning {
	return s.warnings
}

func (s *SeaLog) Name() string {
	return "seaflowlog"
}

func (s *SeaLog) Len() int {
	return len(s.data)
}

// seaLogRecord represents data from one time point in a SeaFlow V1 instrument log
type seaLogRecord struct {
	time time.Time
	data string
}

func (sr seaLogRecord) String() string {
	return fmt.Sprintf("%s\r\n%s", sr.logTime(), sr.data)
}

func (sr seaLogRecord) logTime() string {
	tstr := sr.time.UTC().Format(time.RFC3339)
	tstr = tstr[:13] + "-" + tstr[14:16] + "-" + tstr[17:len(tstr)-1] + "+00:00"
	return tstr
}
