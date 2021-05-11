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
	err error
}

func queueDrainer(res <-chan *queueResult) {
	var success, fail int
	fmt.Fprintf(os.Stderr, "\nTransfer results:\n")
	for r := range res {
		if r.err != nil {
			fail++
			fmt.Fprintf(os.Stderr, "\n%v\n", r.err)
		} else {
			success++
		}
		fmt.Fprintf(os.Stderr, "\r%d failed, %d succeeded       \r", fail, success)
	}
	fmt.Fprintf(os.Stderr, "\n\nDone\n\n")
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
	return nil
}

func queueRunner(wg *sync.WaitGroup, sess *session.Session, inChan <-chan *queueCmd, outChan chan<- *queueResult) {
	defer wg.Done()
	for in := range inChan {
		outChan <- queueItem(sess, in)
	}
}

func queueItem(sess *session.Session, in *queueCmd) (out *queueResult) {
	out = &queueResult{}
	s3api := s3.New(sess)

	// see if the object is already in s3, and if it is, get its length
	s3ObjLength, err := getObjectLength(s3api, in)
	if err != nil {
		out.err = err
		return
	}

	// if the item exists in s3, do a HEAD request against the source
	if s3ObjLength >= 0 {
		retries := 3
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
			// server errors should be retried
			if headRes.StatusCode >= 500 {
				time.Sleep(5 * time.Second)
				if retries > 0 {
					retries--
					continue
				}
			}
			// zero-length files aren't a thing
			if headRes.ContentLength == 0 {
				time.Sleep(1 * time.Second)
				if retries > 0 {
					retries--
					continue
				}
			}
			if headRes.StatusCode != 200 {
				out.err = fmt.Errorf("source URL %q returned HEAD status %03d", in.srcURL, headRes.StatusCode)
				return
			}
			if headRes.ContentLength == s3ObjLength {
				out.err = nil
				return // object in s3 is the same length as source, we're done
			}
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
	retries := 3
	for {
		if err := curl(tmpf, in.srcURL); err != nil {
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
	return
}
