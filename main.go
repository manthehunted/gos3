package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	DEBUG   = slog.LevelDebug
	INFO    = slog.LevelInfo
	WARNING = slog.LevelWarn
	ERROR   = slog.LevelError
)

type Logger struct {
	_logger slog.Logger
}

// FIXME: take level from env variables
func NewLogger() Logger {
	j := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: INFO}))
	jj := Logger{_logger: *j}
	return jj
}

func (l *Logger) Fatalln(msg string) {
	l._logger.Error(msg)
	os.Exit(1)
}

func (l *Logger) Println(msg string) {
	l._logger.Info(msg)
}

func (l *Logger) Debug(msg string, v ...any) {
	l._logger.Debug(msg)
}

func (l *Logger) Warn(msg string, v ...any) {
	l._logger.Warn(msg)
}

type Task struct {
	Key          *string    `json:"key"`
	Bucket       *string    `json:"bucket"`
	Size         *int64     `json:"size"`
	LastModified *time.Time `json:"lastmodified"`
}

func (t *Task) String() string {
	return fmt.Sprintf("%s||%s", *t.Bucket, *t.Key)
}

// ListObjects lists the objects in a bucket.
func ListObjects(ctx context.Context, cfg *aws.Config, bucket *string, prefix *string, logger *Logger) iter.Seq[Task] {

	return func(yield func(d Task) bool) {
		var err error
		var output *s3.ListObjectsV2Output

		client := s3.NewFromConfig(*cfg)
		input := &s3.ListObjectsV2Input{
			Bucket:       aws.String(*bucket),
			Prefix:       aws.String(*prefix),
			RequestPayer: types.RequestPayerRequester,
		}
		objectPaginator := s3.NewListObjectsV2Paginator(client, input)
		for objectPaginator.HasMorePages() {
			output, err = objectPaginator.NextPage(ctx)
			if err != nil {
				var noBucket *types.NoSuchBucket
				if errors.As(err, &noBucket) {
					logger.Warn(fmt.Sprintf("Bucket %s does not exist.\n", *bucket))
					err = noBucket
				}
				break
			} else {
				for _, obj := range output.Contents {
					task := Task{Key: obj.Key, Bucket: bucket, Size: obj.Size, LastModified: obj.LastModified}
					if !yield(task) {
						return
					}
				}
			}
		}
	}
}

var wg sync.WaitGroup

type Path = string

func save(readClose io.ReadCloser, fs string) (Path, error) {
	f, err := os.Create(fs)
	defer f.Close()
	defer readClose.Close()

	if err != nil {
		return fs, fmt.Errorf("create file with err=%s", err)
	}

	wo := bufio.NewWriter(f)
	ri := bufio.NewReader(readClose)
	buf := make([]byte, 1024)
	for {
		n, err := ri.Read(buf)
		if err == io.EOF {
			break
		} else if err != nil {
			return fs, fmt.Errorf("read with err=%s", err)
		}
		if n == 0 {
			break
		}
		n, err = wo.Write(buf[:n])
		if err != nil {
			return fs, fmt.Errorf("write with err=%s", err)
		}
		if err = wo.Flush(); err != nil {
			return fs, fmt.Errorf("flush with err=%s", err)
		}
	}
	if len(buf) > 0 {
		if err = wo.Flush(); err != nil {
			return fs, fmt.Errorf("last flush with err=%s", err)
		}
	}
	return fs, nil
}

func processData(wg *sync.WaitGroup, task Task, cfg *aws.Config, logger *Logger) error {
	defer wg.Done()
	ctx := context.Background()

	client := s3.NewFromConfig(*cfg)

	name := task.Key
	output, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:       task.Bucket,
		Key:          name,
		RequestPayer: types.RequestPayerRequester,
	})
	logger.Println(*name)

	if err != nil {
		return fmt.Errorf("get object from bucket %s with key %s, failed %s", *task.Bucket, *task.Key, err.Error())
	}

	local := strings.Split(*name, "/")
	splits := len(local)
	if splits > 0 {
		local := local[splits-1]
		_, err := save(output.Body, local)
		if err != nil {
			logger.Warn(fmt.Sprintf("while saving %s %s", local, err.Error()))
		}
		logger.Debug("done writing")
		return nil
	} else {
		return fmt.Errorf("unexpected path=%s", *name)
	}
}

func main() {
	bucket := aws.String(os.Args[1])
	prefix := aws.String(os.Args[2])

	logger := NewLogger()
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		logger.Fatalln(err.Error())
	}

	ctx := context.Background()
	for task := range ListObjects(ctx, &cfg, bucket, prefix, &logger) {
		wg.Add(1)
		go processData(&wg, task, &cfg, &logger)
	}
	logger.Println("done sending")

	wg.Wait()
}
