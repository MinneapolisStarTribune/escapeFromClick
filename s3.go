package main

import (
	"fmt"
	"io"
	"strings"
	"unicode"

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
	_, err := upl.Upload(&s3manager.UploadInput{
		Bucket: aws.String(in.destBucket),
		Key:    aws.String(in.destObjName),
		Body:   r,
	})
	if err != nil {
		return fmt.Errorf("cannot upload into %q: %w", in.destObjName, err)
	}
	return nil
}

// https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/Using_Tags.html#tag-restrictions
func s3TagValueFilter(r rune) rune {
	if unicode.In(r, unicode.Letter, unicode.Number, unicode.Space) {
		return r
	}
	// + - = . _ : / @
	switch r {
	case '+', '-', '=', '.', '_', ':', '/', '@':
		return r
	}

	return -1
}

func s3Tag(k, v string) *s3.Tag {
	t := new(s3.Tag)
	t.SetKey(fmt.Sprintf("%s:%s", tagPrefix, k))
	s := strings.Map(s3TagValueFilter, strings.ToValidUTF8(v, ""))
	if len(s) > 254 {
		s = strings.ToValidUTF8(s[0:250]+"...", "")
	}
	t.SetValue(s)
	return t
}

func s3Tagset(in *queueCmd) *s3.Tagging {
	return &s3.Tagging{
		TagSet: []*s3.Tag{
			s3Tag("original", in.srcURL),
			s3Tag("id", in.tagId),
			s3Tag("title", in.tagTitle),
			s3Tag("description", in.tagDescription),
			s3Tag("author", in.tagAuthor),
			s3Tag("credit", in.tagCredit),
			s3Tag("copyright", in.tagCopyright),
		},
	}
}

func s3PutTags(svc *s3.S3, in *queueCmd) error {
	ts := s3Tagset(in)
	_, err := svc.PutObjectTagging(&s3.PutObjectTaggingInput{
		Bucket:  aws.String(in.destBucket),
		Key:     aws.String(in.destObjName),
		Tagging: ts,
	})
	if err != nil {
		return fmt.Errorf("cannot tag %q with %#v: %w", in.destObjName, ts, err)
	}
	return nil
}
