// s3repo
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/mitchellh/ioprogress"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("s3repo")

// Example format string. Everything except the message has a custom color
// which is dependent on the log level. Many fields have a custom output
// formatting too, eg. the time returns the hour down to the milli second.
var format = logging.MustStringFormatter(
	"%{color}%{time:15:04:05.000} %{shortfunc} ▶ %{level:.4s} %{id:03x}%{color:reset} %{message}",
)

var region = flag.String("z", "us-east-1", "AWS region")
var bucket = flag.String("b", "", "bucket to query")
var service = flag.String("s", "", "service component to update")
var version = flag.String("r", "0.1", "version to update")
var destination = flag.String("d", "", "destination directory")
var showName = flag.Bool("p", false, "display the name of the downloaded file")
var showProgress = flag.Bool("i", false, "display progress")
var debug = flag.Bool("v", false, "verbose output")

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s (list|update)\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = Usage
	flag.Parse()

	logging.SetFormatter(format)
	if *debug {
		logging.SetLevel(logging.DEBUG, "s3repo")
	} else {
		logging.SetLevel(logging.ERROR, "s3repo")
	}

	if *service == "" {
		fmt.Println("No service name provided")
		os.Exit(1)
	}

	if *bucket == "" {
		fmt.Println("No bucket provided")
		os.Exit(1)
	}

	if flag.NArg() != 1 {
		Usage()
		os.Exit(2)
	}
	command := flag.Arg(0)

	config := aws.NewConfig().
		WithCredentials(credentials.NewEnvCredentials()).
		WithRegion(*region)

	sess := session.New(config)

	svc := s3.New(sess)

	prefix := *service + "-" + *version

	log.Debug("Querying bucket %s with prefix %s", *bucket, prefix)

	resp, err := svc.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(*bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		log.Fatal(err)
	}

	var maxBuild int = 0
	var maxKey = ""
	for _, obj := range resp.Contents {
		key := *obj.Key
		ext := path.Ext(key)
		var basename string
		if ext != "" {
			basename = strings.TrimSuffix(key, ext)
		} else {
			basename = key
		}
		log.Debug("Checking file %s (%s)", key, basename)
		keyComponents := strings.Split(basename, "-")
		buildStr := keyComponents[len(keyComponents)-1]
		build, err := strconv.Atoi(buildStr)
		if err != nil {
			continue
		}
		log.Debug("For file %s, build=%d", key, build)
		if build > maxBuild {
			maxBuild = build
			maxKey = key
		}
	}

	if maxKey == "" {
		var mostRecent *time.Time = nil
		for _, obj := range resp.Contents {
			if mostRecent == nil || obj.LastModified.After(*mostRecent) {
				mostRecent = obj.LastModified
				maxKey = *(obj.Key)
			}
		}
	}

	if maxKey == "" {
		fmt.Println("No files found to update the service " + *service)
		os.Exit(1)
	}

	if command == "list" {
		for _, obj := range resp.Contents {
			key := obj.Key
			if *key == maxKey {
				fmt.Printf("*%s\n", *key)
			} else {
				fmt.Printf(" %s\n", *key)
			}
		}
	} else if command == "update" {

		remote, err := svc.GetObject(&s3.GetObjectInput{
			Bucket: bucket,
			Key:    &maxKey,
		})
		if err != nil {
			log.Fatal(err)
		}

		defer remote.Body.Close()

		if destination != nil {
			err := os.MkdirAll(*destination, 0755|os.ModeDir)
			if err != nil {
				log.Fatal(err)
			}

			destFilePath := path.Join(*destination, maxKey)

			destFile, err := os.Create(destFilePath)
			if err != nil {
				log.Fatal(err)
			}

			defer destFile.Close()

			var progressR io.Reader
			if *showProgress {
				progressR = &ioprogress.Reader{
					Reader: remote.Body,
					Size:   *remote.ContentLength,
					DrawFunc: ioprogress.DrawTerminalf(os.Stdout, func(progress, total int64) string {
						return fmt.Sprintf("%s: %3d%%", *service, progress*100/total)
					}),
				}
			} else {
				progressR = remote.Body
			}

			w := bufio.NewWriter(destFile)
			if _, err := io.Copy(w, progressR); err != nil {
				log.Fatal(err)
			}
			w.Flush()

			if *showName {
				fmt.Println(destFilePath)
			}
		} else {
			fmt.Println("Destination file not provided")
			os.Exit(1)
		}
	}
}
