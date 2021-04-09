package feeds

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"
)

func FindEVTFiles(dir string) (files []string, err error) {
	pattern := "????-??-??T??-??-??[\\-\\+]??-??"
	patterngz := pattern + ".gz"

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
			} else {
				// Look for uncompressed EVT files
				found, matchErr = filepath.Match(patterngz, d.Name())
				if matchErr != nil {
					panic(matchErr)
				}
				if found {
					files = append(files, walkPath)
				}
			}
		}
		return nil
	})
	return files, err
}

type Evt struct {
	i        int // index of next item to emit
	data     []evtFile
	outDir   string
	warnings []Warning
}

func NewEvt(files []string, outDir string) (e *Evt, err error) {
	e = &Evt{i: -1}
	e.data = []evtFile{}
	e.outDir = outDir
	for _, f := range files {
		t, err := timeFromFilename(f)
		if err != nil {
			e.warnings = append(e.warnings, Warning{err: fmt.Errorf("evt: bad timestamp in %s: %v", f, err)})
		}
		ef := evtFile{path: f, time: t}
		e.data = append(e.data, ef)
	}

	// Sort by time, ascending
	sort.SliceStable(e.data, func(i, j int) bool {
		return e.data[i].time.Before(e.data[j].time)
	})

	return e, nil
}

func (e *Evt) Close() (err error) {
	return
}

func (e *Evt) Earliest() (t time.Time) {
	if len(e.data) > 0 {
		t = e.data[0].time
	}
	return
}

func (e *Evt) Emit() (err error) {
	if e.i < 0 {
		return
	}
	doyDir := fmt.Sprintf("%d_%03d", e.data[e.i].time.Year(), e.data[e.i].time.YearDay())
	outDir := filepath.Join(e.outDir, "datafiles", "evt", doyDir)
	if err = os.MkdirAll(outDir, os.ModePerm); err != nil {
		return fmt.Errorf("evt: %v", err)
	}
	outPath := filepath.Join(outDir, filepath.Base(e.data[e.i].path))

	src, err := os.Open(e.data[e.i].path)
	if err != nil {
		return fmt.Errorf("evt: %v", err)
	}
	defer src.Close()

	dst, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("evt: %v", err)
	}
	defer dst.Close()

	if _, err = io.Copy(dst, src); err != nil {
		return fmt.Errorf("evt: %v", err)
	}

	return
}

func (e *Evt) Time() (t time.Time) {
	if e.i >= 0 && len(e.data) > 0 {
		t = e.data[e.i].time
	}
	return
}

func (e *Evt) Next() bool {
	if e.i+1 < len(e.data) {
		e.i++
		return true
	}
	return false
}

func (e *Evt) Warnings() []Warning {
	return e.warnings
}

func (e *Evt) Name() string {
	return "evt"
}

func (e *Evt) Len() int {
	return len(e.data)
}

type evtFile struct {
	time time.Time
	path string
}

func (ef evtFile) String() string {
	return fmt.Sprintf("%v %s", ef.time, ef.path)
}
