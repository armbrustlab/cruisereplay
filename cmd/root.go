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
package cmd

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/seaflow-uw/cruisereplay/feeds"
	"github.com/spf13/cobra"
)

var Version string = "v0.1.0"

var logger *log.Logger

// flag variables
var (
	evtDirFlag           string
	underwayFileFlag     string
	instrumentLogFlag    string
	startFlag            string
	warpFlag             float64
	outDirFlag           string
	udpPortFlag          uint
	udpHostFlag          string
	underwayThrottleFlag int64
	versionFlag          bool
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "cruisereplay",
	Short: "Replay data feeds for an oceanography cruise",
	Long: `Cruisereplay is a tool to replay data feeds for an oceanography cruise.

Supported data feeds are:
  * SeaFlow EVT
  * SeaFlow SFL
  # SeaFlow instrument log data
  * Kilo Moana underway data`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		var err error

		if versionFlag {
			fmt.Printf("cruisereplay %s\n", Version)
			return
		}

		logger.Printf("-------------------------------------------------------\n")
		logger.Printf("CLI options\n")
		logger.Printf("-------------------------------------------------------\n")
		logger.Printf("--evt = %v\n", evtDirFlag)
		logger.Printf("--underway = %v\n", underwayFileFlag)
		logger.Printf("--seaflowlog = %v\n", instrumentLogFlag)
		logger.Printf("--host = %v\n", udpHostFlag)
		logger.Printf("--port = %v\n", udpPortFlag)
		logger.Printf("--throttle = %vs\n", underwayThrottleFlag)
		var cruiseStart time.Time
		if startFlag != "" {
			cruiseStart, err = time.Parse(time.RFC3339, startFlag)
			if err != nil {
				logger.Fatalf("error: --start: %v\n", err)
			}
			logger.Printf("--start = %v\n", cruiseStart)
		} else {
			logger.Printf("--start = ")
		}
		logger.Printf("-------------------------------------------------------\n")
		logger.Printf("\n")

		// EVT feed
		logger.Printf("-------------------------------------------------------\n")
		logger.Printf("Reading EVT data\n")
		logger.Printf("-------------------------------------------------------\n")
		evtFiles, err := feeds.FindEVTFiles(evtDirFlag)
		if err != nil {
			logger.Fatalf("%v", err)
		}
		evtData, err := feeds.NewEvt(evtFiles, outDirFlag)
		if err != nil {
			logger.Fatalf("%v", err)
		}
		if len(evtData.Warnings()) > 0 {
			for _, w := range evtData.Warnings() {
				logger.Printf("%v", w)
			}
			logger.Printf("-------------------------------------------------------\n")
		}
		logger.Printf("\n")

		// SFL feed
		logger.Printf("-------------------------------------------------------\n")
		logger.Printf("Reading SFL data\n")
		logger.Printf("-------------------------------------------------------\n")
		sflFiles, err := feeds.FindSFLFiles(evtDirFlag)
		if err != nil {
			logger.Fatalf("%v", err)
		}
		sflData, err := feeds.NewSfl(sflFiles, outDirFlag)
		if err != nil {
			logger.Fatalf("%v", err)
		}
		if len(sflData.Warnings()) > 0 {
			for _, w := range sflData.Warnings() {
				logger.Printf("%v", w)
			}
			logger.Printf("-------------------------------------------------------\n")
		}
		logger.Printf("\n")

		// Underway feed
		logger.Printf("-------------------------------------------------------\n")
		logger.Printf("Reading underway data\n")
		logger.Printf("-------------------------------------------------------\n")
		underwayData, err := feeds.NewUnderway(
			underwayFileFlag, udpHostFlag, udpPortFlag, underwayThrottleFlag,
		)
		if err != nil {
			logger.Fatalf("%v", err)
		}
		if len(underwayData.Warnings()) > 0 {
			for _, w := range underwayData.Warnings() {
				logger.Printf("%v", w)
			}
			logger.Printf("-------------------------------------------------------\n")
		}
		logger.Printf("\n")

		// SeaFlow instrument log feed
		logger.Printf("-------------------------------------------------------\n")
		logger.Printf("Reading SeaFlow log data\n")
		logger.Printf("-------------------------------------------------------\n")
		seaflogData, err := feeds.NewSeaLog(instrumentLogFlag, outDirFlag)
		if err != nil {
			logger.Fatalf("%v", err)
		}
		if len(seaflogData.Warnings()) > 0 {
			for _, w := range seaflogData.Warnings() {
				logger.Printf("%v", w)
			}
			logger.Printf("-------------------------------------------------------\n")
		}
		logger.Printf("\n")

		emitters := []feeds.Emitter{evtData, sflData, underwayData, seaflogData}

		// ***************************************************************
		// Calculate time translations between cruise time and replay time
		// ***************************************************************
		// Cruise-time start
		if cruiseStart.IsZero() {
			cruiseStart = minTime(emitters)
		}
		delay, err := time.ParseDuration("5s")
		if err != nil {
			panic(err)
		}
		// Replay-time start with small delay
		replayStart := time.Now().Add(delay)

		logger.Printf("cruise start = %v\n", cruiseStart)
		logger.Printf("replay cruise start = %v\n", replayStart)

		done := make(chan bool)

		for _, e := range emitters {
			go startEmitter(e, cruiseStart, replayStart, warpFlag, done)
			defer e.Close()
		}

		logger.Printf("waiting on %d feeds\n", len(emitters))
		for range emitters {
			<-done
		}
		fmt.Println("all feeds complete, closing")

		logger.Printf("exiting")
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	cobra.CheckErr(rootCmd.Execute())
}

func init() {
	logger = log.New(os.Stderr, "", 0)

	rootCmd.PersistentFlags().StringVar(&evtDirFlag, "evt", "", "EVT directory")
	cobra.MarkFlagRequired(rootCmd.PersistentFlags(), "evt")
	rootCmd.PersistentFlags().StringVar(&underwayFileFlag, "underway", "", "underway raw feed file")
	cobra.MarkFlagRequired(rootCmd.PersistentFlags(), "underway")
	rootCmd.PersistentFlags().StringVar(&instrumentLogFlag, "seaflowlog", "", "SeaFlow instrument log file")
	cobra.MarkFlagRequired(rootCmd.PersistentFlags(), "seaflowlog")
	rootCmd.PersistentFlags().StringVar(&outDirFlag, "outdir", "",
		"output directory")
	cobra.MarkFlagRequired(rootCmd.PersistentFlags(), "outdir")
	rootCmd.PersistentFlags().StringVar(&startFlag, "start", "",
		"RFC3339 timestamp for replay start, in cruise time")
	rootCmd.PersistentFlags().Float64Var(&warpFlag, "warp", 1.0,
		"time speedup/slowdown factor")
	rootCmd.PersistentFlags().UintVar(&udpPortFlag, "port", 5555, "UDP destination port")
	rootCmd.PersistentFlags().StringVar(&udpHostFlag, "host", "255.255.255.255", "UDP destination IP address")
	rootCmd.PersistentFlags().Int64Var(&underwayThrottleFlag, "throttle", 60, "produce UDP feed data at most every N sec")
	rootCmd.PersistentFlags().BoolVar(&versionFlag, "version", false, "print version and exit")
}

func minTime(es []feeds.Emitter) (first time.Time) {
	for _, e := range es {
		logger.Printf("%v\n", e.Earliest())
		if first.IsZero() || e.Earliest().Before(first) {
			first = e.Earliest()
		}
	}
	return
}

func startEmitter(e feeds.Emitter, cruiseStart, replayStart time.Time, warp float64, done chan bool) {
	for e.Next() {
		if e.Time().Before(cruiseStart) {
			continue
		}
		// Duration between cruise start with offset and this point
		delta := e.Time().Sub(cruiseStart)
		// Adjust for time warp
		delta = time.Duration(float64(delta.Nanoseconds()) / warp)
		if delta < 0 {
			panic(fmt.Errorf("delta < 0, %v, for %v", delta, e.Time()))
		}
		emitTime := replayStart.Add(delta) // when to emit
		untilEmit := time.Until(emitTime)  // how long until emit
		logger.Printf("%v timer set for %v in %v\n", e.Name(), emitTime.UTC(), untilEmit)
		timer := time.NewTimer(untilEmit)
		<-timer.C
		logger.Printf("%v timer fired at %v\n", e.Name(), time.Now().UTC())
		err := e.Emit()
		if err != nil {
			log.Printf("%v", err)
		}
	}
	done <- true
}
