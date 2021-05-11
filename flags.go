package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

func env(key, def string) string {
	if r, ok := os.LookupEnv(key); ok {
		return r
	}
	return def
}

var xmlFile string
var awsRegion string
var srcHTTPRoot string
var dstS3Bucket string
var dstS3Path string
var maxThreads int
var tmpLocation string

func init() {
	flag.StringVar(&xmlFile, "xmlfile", "", "XML file to ingest for transfer")
	flag.StringVar(&awsRegion, "region", env("AWS_REGION", "us-west-2"), "AWS Region")
	flag.StringVar(&srcHTTPRoot, "src", "https://www.example.net", "Source file location")
	flag.StringVar(&dstS3Bucket, "dst", "exampleBucket", "Destination S3 Bucket")
	flag.StringVar(&dstS3Path, "dstPath", "/", "Destination S3 Bucket Path")
	flag.StringVar(&tmpLocation, "tmp", os.TempDir(), "Location for storing files until upload")
	flag.IntVar(&maxThreads, "threads", 1, "Maximum number of items to download/upload in parallel")
	flag.Parse()

	if xmlFile == "" {
		fmt.Fprintf(os.Stderr, "No input XML file specified. Rerun as\n%s -xmlfile exported.xml\n", os.Args[0])
		os.Exit(1)
	}

	if maxThreads < 1 {
		fmt.Fprintf(os.Stderr, "Thread count of %d is invalid, specify at least 1\n", maxThreads)
		os.Exit(1)
	}

	if strings.HasSuffix(srcHTTPRoot, "/") {
		fmt.Fprintf(os.Stderr, "The source URL (%q) must not end in a forward slash\n", srcHTTPRoot)
		os.Exit(1)
	}

	if !strings.HasPrefix(srcHTTPRoot, "http") {
		fmt.Fprintf(os.Stderr, "The source URL (%q) must start with \"http://\" or \"https://\"\n", srcHTTPRoot)
		os.Exit(1)
	}

	if strings.HasSuffix(dstS3Bucket, "/") {
		fmt.Fprintf(os.Stderr, "The destination bucket (%q) must not end in a forward slash\n", dstS3Bucket)
		os.Exit(1)
	}

	if !strings.HasSuffix(dstS3Path, "/") {
		fmt.Fprintf(os.Stderr, "The destination path (%q) must end in a forward slash\n", dstS3Path)
		os.Exit(1)
	}
}
