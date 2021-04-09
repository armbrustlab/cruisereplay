package feeds

import (
	"bufio"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func FindSFLFiles(dir string) (files []string, err error) {
	pattern := "????-??-??T??-??-??[\\-\\+]??-??.sfl"

	err = filepath.WalkDir(dir, func(walkPath string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if d == nil {
				// Failed fs.Stat on root dir
				return walkErr
			}
			// ReadDir failed on this directory
			fmt.Fprintf(os.Stderr, "error: %v\n", walkErr)
			return nil
		}
		if !d.IsDir() {
			// Look for uncompressed EVT files
			found, matchErr := filepath.Match(pattern, d.Name())
			if matchErr != nil {
				panic(matchErr)
			}
			if found {
				files = append(files, walkPath)
			}
		}
		return nil
	})
	return files, err
}

// *****************************************************************************
type Sfl struct {
	i        int // index of next item to emit
	data     []sflRecord
	paths    []string
	outDir   string
	file     *os.File // current output file
	warnings []Warning
}

func NewSfl(files []string, outDir string) (s *Sfl, err error) {
	s = &Sfl{i: -1}
	s.data = []sflRecord{}
	s.outDir = outDir
	for idx, f := range files {
		fileText, err := ioutil.ReadFile(f)
		if err != nil {
			return s, err
		}
		sc := bufio.NewScanner(strings.NewReader(string(fileText)))
		lineNum := 0
		var header string
		for sc.Scan() {
			lineNum++
			lineText := sc.Text()
			if lineNum == 1 {
				s.paths = append(s.paths, f)
				header = sc.Text()
				continue
			}
			cols := strings.Split(lineText, "\t")
			if len(cols) > 1 && len(cols[0]) == 25 {
				tstamp := cols[0][:19] + "+00:00" // TZ untrustworthy, force UTC
				tstamp = tstamp[:13] + ":" + tstamp[14:16] + ":" + tstamp[17:]
				lineTime, err := time.Parse(time.RFC3339, tstamp)
				if err != nil {
					// Skip this line
					newErr := fmt.Errorf("sfl: could not parse timestamp %s:%d %v", f, lineNum, err)
					s.warnings = append(s.warnings, Warning{err: newErr})
					continue
				}
				if lineNum == 2 {
					lineText = header + "\r\n" + lineText
				}
				s.data = append(s.data, sflRecord{time: lineTime, data: lineText, idx: idx})
			} else {
				newErr := fmt.Errorf("sfl: unparsable line %s:%d", f, lineNum)
				s.warnings = append(s.warnings, Warning{err: newErr})
			}
		}
	}

	// Sort by time, ascending
	sort.SliceStable(s.data, func(i, j int) bool {
		return s.data[i].time.Before(s.data[j].time)
	})

	return s, nil
}

func (s *Sfl) Close() (err error) {
	if s.file != nil {
		err = s.Close()
		s = nil
	}
	return
}

func (s *Sfl) Earliest() (t time.Time) {
	if len(s.data) > 0 {
		t = s.data[0].time
	}
	return
}

func (s *Sfl) Emit() (err error) {
	if s.i < 0 {
		return
	}
	rec := s.data[s.i]
	outFileTime, err := timeFromFilename(s.paths[rec.idx])
	if err != nil {
		return fmt.Errorf("sfl: %v", err)
	}
	doyDir := fmt.Sprintf("%d_%03d", outFileTime.Year(), outFileTime.YearDay())
	outDir := filepath.Join(s.outDir, "datafiles", "evt", doyDir)
	if err = os.MkdirAll(outDir, os.ModePerm); err != nil {
		return fmt.Errorf("sfl: %v", err)
	}
	outPath := filepath.Join(outDir, filepath.Base(s.paths[rec.idx]))
	if s.file == nil || s.file.Name() != outPath {
		if err = s.Close(); err != nil {
			return fmt.Errorf("sfl: %v", err)
		}
		if s.file, err = os.OpenFile(outPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModePerm); err != nil {
			return fmt.Errorf("sfl: %v", err)
		}
	}
	s.file.WriteString(fmt.Sprintf("%s\r\n", rec.data))
	return
}

func (s *Sfl) Time() (t time.Time) {
	if s.i >= 0 && len(s.data) > 0 {
		t = s.data[s.i].time
	}
	return
}

func (s *Sfl) Next() bool {
	if s.i+1 < len(s.data) {
		s.i++
		return true
	}
	return false
}

func (s *Sfl) Warnings() []Warning {
	return s.warnings
}

func (s *Sfl) Name() string {
	return "sfl"
}

func (s *Sfl) Len() int {
	return len(s.data)
}

// sfl is one data line of an SFL file with a header line prepended if this is
// the first line in a file.
type sflRecord struct {
	time time.Time
	idx  int
	data string
}

func (sr sflRecord) String() string {
	return fmt.Sprintf("%v %s", sr.time, sr.data)
}
