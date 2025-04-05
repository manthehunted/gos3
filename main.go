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
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	DEBUG = slog.LevelDebug
	INFO  = slog.LevelInfo
	WARN  = slog.LevelWarn
	ERROR = slog.LevelError
)

type Logger struct {
	_logger slog.Logger
}

var LogLevel = os.Getenv("LOG_LEVEL")

func NewLogger() Logger {
	var level slog.Leveler
	switch strings.ToLower(LogLevel) {
	case "debug":
		level = DEBUG
	case "info":
		level = INFO
	case "warn":
		level = WARN
	case "error":
		level = ERROR
	case "":
		level = INFO
	default:
		panic(fmt.Sprintf("LOG_LEVEL=%s not supported", LogLevel))
	}
	j := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
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

func assert(s string) {
	if strings.HasPrefix(s, "/") {
		panic(s)
	}
}

type Lister interface {
	List(ctx context.Context, cfg *aws.Config, logger *Logger) iter.Seq[Task]
}

type S3Path struct{ path string }

func NewS3Path(path string) (S3Path, error) {
	if !strings.HasPrefix(path, "s3:/") {
		return S3Path{}, errors.New(fmt.Sprintf("%s does not start with s3:/", path))
	}
	return S3Path{path}, nil
}

func (s3path *S3Path) ToBucketPrefix() (string, string) {
	// NOTE: trim s3:/
	str := s3path.path[4:]
	// practice
	defer func() {
		if r := recover(); r != nil {
			panic(fmt.Sprintf("%s starts with /", r))
		}
	}()
	assert(str)

	i := 0
	lenPath := len(str)
	for i < lenPath && !os.IsPathSeparator(str[i]) {
		i++
	}
	return str[:i], str[i+1:]
}

func (s3path *S3Path) List(ctx context.Context, cfg *aws.Config, logger *Logger) iter.Seq[Task] {
	bucket, prefix := s3path.ToBucketPrefix()
	return ListObjects(ctx, cfg, &bucket, &prefix, logger)
}

type LocalFile struct{ path string }

func (fs *LocalFile) List(ctx context.Context, cfg *aws.Config, logger *Logger) iter.Seq[Task] {
	fd, err := os.Open(fs.path)
	if err != nil {
		panic(fmt.Sprintf("cannot read path=%s with error=%s", fs.path, err))
	}
	scanner := bufio.NewScanner(fd)
	return func(yield func(d Task) bool) {
		defer func() {
			if r := recover(); r != nil {
				logger.Warn(fmt.Sprint(r))
			}
		}()
		for scanner.Scan() {
			s3path, err := NewS3Path(scanner.Text())
			if err != nil {
				logger.Warn(err.Error())
				continue
			}
			bucket, prefix := s3path.ToBucketPrefix()
			for k := range ListObjects(ctx, cfg, &bucket, &prefix, logger) {
				if !yield(k) {
					return
				}
			}
		}
	}
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
				} else {
					logger.Warn(fmt.Sprintf("got err while pagination=%s\n", err))
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

func save(readClose io.ReadCloser, fs string) (string, error) {
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
	// Support
	// s3 s3:/test/prefix
	// s3 s3:/test/prefix/t.go
	// s3 file.txt
	// where file.txt is \n separated
	// contains s3 path
	logger := NewLogger()

	var ls Lister
	filename := filepath.Clean(os.Args[1])
	if strings.HasPrefix(filename, "s3:/") {
		ls = &S3Path{filename}
	} else if strings.HasSuffix(filename, ".txt") {
		filepath.IsLocal(filename)
		ls = &LocalFile{filename}
	} else {
		panic("only support file extension is .txt or string starts with s3:/")
	}

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		logger.Fatalln(err.Error())
	}

	var wg sync.WaitGroup
	ctx := context.Background()
	for task := range ls.List(ctx, &cfg, &logger) {
		wg.Add(1)
		go processData(&wg, task, &cfg, &logger)
	}
	logger.Println("done")

	wg.Wait()
}
