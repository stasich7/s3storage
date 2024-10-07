package s3storage

import (
	"fmt"
	"os"
	"regexp"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3Storage struct {
	S3       *s3.S3
	Bucket   string
	S3Public string
}

type S3StorageConfig struct {
	S3AccessKey string
	S3SecretKey string
	S3Region    string
	S3Bucket    string
	S3APIURL    string
	S3Public    string
}

// package exported functions
func New(conf S3StorageConfig) (*S3Storage, error) {
	sess, err := session.NewSession(&aws.Config{
		Endpoint:    aws.String(conf.S3APIURL),
		Region:      aws.String(conf.S3Region),
		Credentials: credentials.NewStaticCredentials(conf.S3AccessKey, conf.S3SecretKey, ""),
	})
	if err != nil {
		return nil, err
	}
	svc := s3.New(sess)

	return &S3Storage{
		S3:       svc,
		Bucket:   conf.S3Bucket,
		S3Public: conf.S3Public,
	}, nil
}

func (svc *S3Storage) S3put(fromfile, location, filename string) (string, error) {
	f, err := os.OpenFile(fromfile, os.O_RDONLY, 0)
	if err != nil {
		return "", err
	}

	putInput := &s3.PutObjectInput{
		Bucket: aws.String(svc.Bucket),
		Key:    aws.String(fmt.Sprintf("%s/%s", location, filename)),
		Body:   aws.ReadSeekCloser(f),
	}

	_, err = svc.S3.PutObject(putInput)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s/%s", svc.S3Public, *putInput.Key), nil
}

func (svc *S3Storage) S3delete(location, filename string) error {
	if filename == "" || filename == "*" || filename == "/" || filename == "." || filename == ".." {
		return fmt.Errorf("wrong filename")
	}
	delInput := &s3.DeleteObjectInput{
		Bucket: aws.String(svc.Bucket),
		Key:    aws.String(fmt.Sprintf("%s/%s", location, filename)),
	}

	_, err := svc.S3.DeleteObject(delInput)
	if err != nil {
		return err
	}

	return nil
}

func (svc *S3Storage) S3get(location, filename string) (output *s3.GetObjectOutput, publicLocation string, err error) {
	getInput := &s3.GetObjectInput{
		Bucket: aws.String(svc.Bucket),
		Key:    aws.String(fmt.Sprintf("%s/%s", location, filename)),
	}

	output, err = svc.S3.GetObject(getInput)
	if err != nil {
		return nil, "", err
	}

	return output, fmt.Sprintf("%s/%s", svc.S3Public, *getInput.Key), nil
}

func (svc *S3Storage) S3List(mask string) error {
	input := &s3.ListObjectsInput{
		Bucket:  aws.String(svc.Bucket),
		MaxKeys: aws.Int64(10),
	}

	result, err := svc.S3.ListObjects(input)
	if err != nil || result == nil {
		return err
	}

	if mask != "" {
		re, err := regexp.Compile(mask)
		if err != nil {
			return err
		}
		for i := range result.Contents {
			if re.MatchString(*result.Contents[i].Key) {
				fmt.Println(result.Contents[i])
			}
		}
	} else {
		fmt.Println(result)
	}
	return nil
}
