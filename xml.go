package main

import (
	"encoding/xml"
	"fmt"
	"io"
)

type cmMedia struct {
	XMLName     xml.Name `xml:"media"`
	Id          string   `xml:"id,attr"`
	Path        string   `xml:"path"`
	Title       string   `xml:"title"`
	Description string   `xml:"description"`
	Author      string   `xml:"author"`
	Credit      string   `xml:"credit"`
	Copyright   string   `xml:"copyright"`
}

func queueFeeder(r io.ReadSeeker, out chan<- *queueCmd) error {
	defer close(out)
	item := cmMedia{}
	dec := xml.NewDecoder(r)
	for {
		tok, err := dec.Token()
		if err != nil {
			return fmt.Errorf("cannot find parent cmPublishImport tag: %w", err)
		}
		if el, ok := tok.(xml.StartElement); ok {
			if el.Name.Local == "cmPublishImport" {
				break
			}
		}
	}
	for {
		err := dec.Decode(&item)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("cannot parse xml: %w", err)
		}
		out <- &queueCmd{
			srcURL:         fmt.Sprintf("%s/%s", srcHTTPRoot, item.Path),
			destBucket:     dstS3Bucket,
			destObjName:    fmt.Sprintf("%s%s", dstS3Path, item.Path),
			tagId:          item.Id,
			tagTitle:       item.Title,
			tagDescription: item.Description,
			tagAuthor:      item.Author,
			tagCredit:      item.Credit,
			tagCopyright:   item.Copyright,
		}
	}
}
