/*
Copyright © 2021 University of Washington

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/
package feeds

import (
	"fmt"
	"path/filepath"
	"regexp"
	"time"
)

type Emitter interface {
	Name() string
	Earliest() time.Time
	Next() bool      // move to next item to emit in time series
	Time() time.Time // get time for item to emit
	Emit() error
	Close() error // close any open resources
	Len() int
}

type Warning struct {
	err error
}

func (w Warning) String() string {
	return fmt.Sprintf("%v", w.err)
}

// timeFromFilename parses a SeaFlow timestamped filename. This function assumes
// all times are UTC, even if they have non-UTC timezone designator.
func timeFromFilename(fn string) (time.Time, error) {
	fnbase := filepath.Base(fn)
	re := regexp.MustCompile(`^(\d{4}-\d{2}-\d{2}T\d{2})-(\d{2})-(\d{2}(?:\.\d+)?)(?:.+)?$`)
	subs := re.FindStringSubmatch(fnbase)
	if len(subs) != 4 {
		return time.Time{}, fmt.Errorf("file timtestamp could not be parsed for %v", fn)
	}
	ts := subs[1] + ":" + subs[2] + ":" + subs[3] + "+00:00"
	return time.Parse(time.RFC3339, ts)
}
