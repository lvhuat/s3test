package s3file

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/sirupsen/logrus"
)

var (
	bucket = "bu-prod01"
	prefix = "s3-test"
)

var (
	charset = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

var rander *rand.Rand

func init() {
	rander = rand.New(rand.NewSource(time.Now().UnixNano()))
}

// RandomString 随机
func RandomString(n int) string {
	b := make([]byte, n)
	for index := 0; index < n; index++ {
		b[index] = charset[rander.Intn(len(charset))]
	}
	return string(b)
}

type S3File struct {
	reader *s3.S3
	writer *s3.S3
}

func (p *S3File) Init() {
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(endpoints.UsWest2RegionID),
	}))

	p.writer = s3.New(sess, &aws.Config{
		Region: aws.String(endpoints.UsWest2RegionID),
	})

	sess = session.Must(session.NewSession(&aws.Config{
		Region: aws.String(endpoints.UsWest2RegionID),
	}))

	p.reader = s3.New(sess, &aws.Config{
		Region: aws.String(endpoints.UsWest2RegionID),
	})
}

func (p *S3File) Write(key string, size int, content []byte) error {
	timeout := 10 * time.Minute

	/*sess := session.Must(session.NewSession(&aws.Config{
	          Region: aws.String(endpoints.UsWest2RegionID),
	  }))

	  svc := s3.New(sess, &aws.Config{
	          Region: aws.String(endpoints.UsWest2RegionID),
	  })*/

	ctx := context.Background()
	var cancelFn func()
	if timeout > 0 {
		ctx, cancelFn = context.WithTimeout(ctx, timeout)
	}

	if cancelFn != nil {
		defer cancelFn()
	}

	// generate empty payload

	_, err := p.writer.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(prefix + key),
		Body:   bytes.NewReader(content), //Body:   os.Stdin,
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
			logrus.WithError(err).Info("upload canceled due to timeout")
		} else {
			logrus.WithError(err).Info("failed to upload object")
		}
		return err
	}

	/*logrus.WithFields(logrus.Fields{
	          "key":   key,
	          "size":  size,
	          //"content":  content,
	  }).Info("write success")
	*/

	return nil
}

//s3://bu-prod01/s3-test/accountId_id_data
func (p *S3File) Read(key string) ([]byte, error) {

	ctx := context.Background()

	result, err := p.reader.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(prefix + key),
	})

	if err != nil {
		aerr, ok := err.(awserr.Error)
		if ok && aerr.Code() == s3.ErrCodeNoSuchKey {
			// Specific error code handling
		}

		return nil, err
	}

	// Make sure to close the body when done with it for S3 GetObject APIs or
	// will leak connections.
	defer result.Body.Close()

	return ioutil.ReadAll(result.Body)
}

func BenchTestWrite(sessions []*S3File, total, routines int, name string) {
	size := VALUE_SIZE
	ch := make(chan string, 10000)

	data := []byte(RandomString(size))
	success := int64(0)
	totalBytes := int64(0)
	var minTakeTime time.Duration
	var maxTakeTime time.Duration = time.Hour
	// 生产者
	go func() {
		for i := 0; i < total; i++ {
			ch <- fmt.Sprintf("%s/%s/%05d", prefix, name, i)
		}
		// 给出结束信号
		close(ch)
	}()

	wg := sync.WaitGroup{}
	wg.Add(routines)
	since := time.Now()
	for i := 0; i < routines; i++ {
		go func(n int) {
			defer wg.Done()
			for {
				key, ok := <-ch
				if !ok {
					return
				}
				tm1 := time.Now()
				if err := sessions[n].Write(key, size, data); err != nil {
					continue
				}
				takeTime := time.Since(tm1)

				if takeTime > maxTakeTime {
					maxTakeTime = takeTime
				}
				if takeTime < minTakeTime {
					minTakeTime = takeTime
				}

				atomic.AddInt64(&success, 1)
				atomic.AddInt64(&totalBytes, int64(len(data)))
			}
		}(i)
	}
	wg.Wait()

	logrus.WithFields(logrus.Fields{
		"routines":     routines,
		"fileSize":     size,
		"totalFiles":   total,
		"costTime":     time.Since(since),
		"successFiles": success,
		"totalBytes":   totalBytes,
		"minTakeTime":  minTakeTime,
		"maxTakeTime":  maxTakeTime,
	}).Info("Loop write")

}

func BenchTestRead(sessions []*S3File, total, routines int, name string) {
	ch := make(chan string, 10000)

	// 生产者
	go func() {
		for i := 0; i < total; i++ {
			ch <- fmt.Sprintf("%s/%s/%05d", prefix, name, i)
		}
		// 给出结束信号
		close(ch)
	}()

	success := int64(0)
	totalBytes := int64(0)

	var minTakeTime time.Duration
	var maxTakeTime time.Duration = time.Hour

	wg := sync.WaitGroup{}
	wg.Add(routines)
	since := time.Now()
	for i := 0; i < routines; i++ {
		go func(n int) {
			defer wg.Done()
			for {
				key, ok := <-ch
				if !ok {
					return
				}

				tm1 := time.Now()

				body, err := sessions[n].Read(key)
				if err != nil {
					logrus.WithError(err).WithField("key", key).Infoln("Read error")
					continue
				}
				takeTime := time.Since(tm1)

				if takeTime > maxTakeTime {
					maxTakeTime = takeTime
				}
				if takeTime < minTakeTime {
					minTakeTime = takeTime
				}

				atomic.AddInt64(&totalBytes, int64(len(body)))
				atomic.AddInt64(&success, 1)
			}
		}(i)
	}
	wg.Wait()

	logrus.WithFields(logrus.Fields{
		"routines":     routines,
		"totalFiles":   total,
		"costTime":     time.Since(since),
		"successFiles": success,
		"totalBytes":   totalBytes,
		"minTakeTime":  minTakeTime,
		"maxTakeTime":  maxTakeTime,
	}).Info("Read done")
}

var (
	VALUE_SIZE = 1024 * 1332 //1.3M
)

func Loop(total, num int, name string) {
	//num := 100
	if name == "" {
		name = fmt.Sprintf("%d", rand.Int31())
	}

	count := total / num

	fmt.Printf("thread num: %d , name: %s, total: %d, count: %d \n", num, name, total, count)

	objs := make([]*S3File, num)
	for i := 0; i < num; i++ {
		objs[i] = new(S3File)
		objs[i].Init()
	}

	BenchTestWrite(objs, total, num, name)
	BenchTestRead(objs, total, num, name)
}
