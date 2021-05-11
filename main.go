package main

import (
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
)

func main() {
	xmlfp, err := os.Open(xmlFile)
	if err != nil {
		panic(err)
	}
	defer xmlfp.Close()

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(awsRegion),
	})
	if err != nil {
		panic(err)
	}

	err = startQueue(sess, xmlfp)
	if err != nil {
		panic(err)
	}
}
