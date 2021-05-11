package main

import (
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// getObjectLength returns a length of -1 for a missing object. I know, I know,
// sentinel values are bad. But 404 is not an exceptional case and instead we
// want to return an error only if there is some other unrecoverable error.
func getObjectLength(svc *s3.S3, in *queueCmd) (int64, error) {
	objAttrs, err := svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(in.destBucket),
		Key:    aws.String(in.destObjName),
	})
	if err == nil {
		return *objAttrs.ContentLength, nil
	}
	if amzerr, ok := err.(awserr.Error); ok {
		if reqerr, ok := amzerr.(awserr.RequestFailure); ok {
			if reqerr.StatusCode() == 404 {
				// yep, sentinel value, sorry -- I was raised on QBasic
				return -1, nil
			}
			return 0, fmt.Errorf("cannot retrieve metadata for %q: request error: %w", in.destObjName, reqerr)
		}
		return 0, fmt.Errorf("cannot retrieve metadata for %q: amazon error: %w", in.destObjName, amzerr)
	}
	return 0, fmt.Errorf("cannot retrieve metadata for %q: general error: %w", in.destObjName, err)
}

func putObject(sess *session.Session, in *queueCmd, r io.ReadSeeker) error {
	upl := s3manager.NewUploader(sess)
	md := map[string]*string{
		"original":       &in.srcURL,
		"clickabilityid": &in.tagId,
	}
	if len(in.tagTitle) > 0 {
		md["title"] = &in.tagTitle
	}
	if len(in.tagDescription) > 0 {
		md["description"] = &in.tagDescription
	}
	if len(in.tagAuthor) > 0 {
		md["author"] = &in.tagAuthor
	}
	if len(in.tagCredit) > 0 {
		md["credit"] = &in.tagCredit
	}
	if len(in.tagCopyright) > 0 {
		md["copyright"] = &in.tagCopyright
	}

	_, err := upl.Upload(&s3manager.UploadInput{
		Bucket:   aws.String(in.destBucket),
		Key:      aws.String(in.destObjName),
		Body:     r,
		Metadata: md,
	})
	if err != nil {
		return fmt.Errorf("cannot upload into %q: %w", in.destObjName, err)
	}
	return nil
}
