package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type queueCmd struct {
	srcURL         string
	destBucket     string
	destObjName    string
	tagId          string
	tagTitle       string
	tagDescription string
	tagAuthor      string
	tagCredit      string
	tagCopyright   string
}

type queueResult struct {
	cmd       *queueCmd
	changed   bool
	skipped   bool
	succeeded bool
	err       error
	warn      error
}

func queueDrainer(res <-chan *queueResult) {
	var total, downloaded, changed, skipped, failed, warned int
	fmt.Fprintf(os.Stdout, "Transfer results:\n")
	for r := range res {
		total++
		if r.err != nil {
			failed++
			fmt.Fprintf(os.Stdout, "%s: %v\n", r.cmd.tagId, r.err)
		} else if r.warn != nil {
			warned++
			fmt.Fprintf(os.Stdout, "%s: %v\n", r.cmd.tagId, r.warn)
		}

		if r.changed {
			changed++
		} else if r.skipped {
			skipped++
		} else if r.succeeded {
			downloaded++
		}
		fmt.Fprintf(os.Stderr, "\r%d total, %d failed, %d warned, %d new, %d changed, %d skipped       \r",
			total, failed, warned, downloaded, changed, skipped)
	}
	fmt.Fprintf(os.Stdout, "\nDone\n")
	fmt.Fprintf(os.Stdout, "Final: %d total, %d failed, %d warned, %d new, %d changed, %d skipped\n",
		total, failed, warned, downloaded, changed, skipped)
}

func startQueue(sess *session.Session, xmlin io.ReadSeeker) error {
	if maxThreads < 1 {
		maxThreads = 1
	}
	in := make(chan *queueCmd)
	out := make(chan *queueResult)
	wg := new(sync.WaitGroup)
	for t := maxThreads; t > 0; t-- {
		wg.Add(1)
		go queueRunner(wg, sess, in, out)
	}
	go queueDrainer(out)
	err := queueFeeder(xmlin, in)
	if err != nil {
		return err
	}
	wg.Wait()
	close(out)
	time.Sleep(1 * time.Second) // ick, sorry
	return nil
}

func queueRunner(wg *sync.WaitGroup, sess *session.Session, inChan <-chan *queueCmd, outChan chan<- *queueResult) {
	defer wg.Done()
	for in := range inChan {
		outChan <- queueItem(sess, in)
	}
}

func queueItem(sess *session.Session, in *queueCmd) (out *queueResult) {
	out = &queueResult{cmd: in}
	s3api := s3.New(sess)

	// see if the object is already in s3, and if it is, get its length
	s3ObjLength, err := getObjectLength(s3api, in)
	if err != nil {
		out.err = err
		return
	}

	var expectLength int64
	// if the item exists in s3, do a HEAD request against the source
	if s3ObjLength > 0 {
		retries := 4
		for {
			headRes, err := http.Head(in.srcURL)
			if err != nil {
				out.err = fmt.Errorf("failed to execute HEAD request for %q: %w", in.srcURL, err)
				return
			}
			if err := headRes.Body.Close(); err != nil {
				out.err = fmt.Errorf("failed to close HEAD response body for %q: %w", in.srcURL, err)
				return
			}
			// all errors should be retried
			if headRes.StatusCode >= 400 {
				time.Sleep(5 * time.Second)
				if retries > 0 {
					retries--
					continue
				}
			}
			// zero-length files aren't a thing
			if headRes.ContentLength <= 0 {
				time.Sleep(1 * time.Second)
				if retries > 0 {
					retries--
					continue
				}
				out.warn = fmt.Errorf("skipping: source URL %q returned HEAD size %d", in.srcURL, headRes.ContentLength)
				out.skipped = true
				return
			}
			if headRes.StatusCode != 200 {
				out.warn = fmt.Errorf("skipping: source URL %q returned HEAD status %03d", in.srcURL, headRes.StatusCode)
				out.skipped = true
				return
			} else if headRes.ContentLength == s3ObjLength {
				out.err = nil
				out.warn = nil
				out.skipped = true
				return // object in s3 is the same length as source, we're done
			} else if headRes.ContentLength < s3ObjLength {
				time.Sleep(1 * time.Second)
				if retries > 0 {
					retries--
					continue
				}
				out.warn = fmt.Errorf("skipping: source %q is smaller than s3 content (%d < %d)",
					in.srcURL, headRes.ContentLength, s3ObjLength)
				out.skipped = true
				return
			}
			expectLength = headRes.ContentLength
			out.changed = true
			break
		}
	}

	// either the object isn't in s3, or the lengths differ, so fetch the source
	tmpf, err := os.CreateTemp(tmpLocation, "s3upload")
	if err != nil {
		out.err = fmt.Errorf("failed to create temporary file: %w", err)
		return
	}
	defer tmpf.Close()
	os.Remove(tmpf.Name()) // immediately unlink temporary file so OS will clean up for us
	retries := 4
	for {
		if err := curl(tmpf, in.srcURL, expectLength); err != nil {
			time.Sleep(5 * time.Second)
			if retries > 0 {
				retries--
				continue
			}
			out.err = fmt.Errorf("cannot download source file: %w", err)
			return
		}
		break
	}
	if err := putObject(sess, in, tmpf); err != nil {
		out.err = fmt.Errorf("cannot upload to s3: %w", err)
		return
	}
	out.succeeded = true
	return
}
