// Copyright © 2019 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package cleaner

import (
	"fmt"
	"github.com/minio/minio-go"
	"github.com/pkg/errors"
	"net/url"
	"sort"
)

// Cleaner represent the logic for cleaning the bucket from old backups.
type Cleaner struct {
	cli *minio.Client
}

// Credentials represents S3 compatible credentials.
type Credentials struct {
	Key    string
	Secret string
	Secure bool
}

// NewCleaner create an instance of backup cleaner.
func NewCleaner(endpoint string, creds Credentials) (*Cleaner, error) {
	cli, err := minio.New(endpoint, creds.Key, creds.Secret, creds.Secure)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create s3 client")
	}
	return &Cleaner{
		cli: cli,
	}, nil
}

// Clean run the cleaning process of bucket from old backup.
func (c *Cleaner) Clean(bucketUrl url.URL, saveLast int) error {
	exist, err := c.cli.BucketExists(bucketUrl.Host)
	if err != nil {
		return errors.Wrap(err, "can't verify that bucket exist")
	}
	if !exist {
		return fmt.Errorf("bucket %s doesn't exist", bucketUrl.Host)
	}

	toClean, err := c.filesToClean(bucketUrl.Host, bucketUrl.Path, saveLast)
	if err != nil {
		return errors.Wrap(err, "failed to get list of files that should be removed")
	}

	delErr := 0
	for rerr := range c.cli.RemoveObjects(bucketUrl.Host, toClean) {
		fmt.Printf("Error detected during deletion object %s: %v", rerr.ObjectName, rerr.Err)
		delErr++
	}
	if delErr != 0 {
		return fmt.Errorf("cleaning process ended with errors")
	}
	return nil
}

func (c *Cleaner) filesToClean(bucketName string, prefix string, saveLast int) (<-chan string, error) {
	files := make(files, 0)
	done := make(chan struct{})

	objects := c.cli.ListObjectsV2(bucketName, prefix, false, done)
	for object := range objects {
		if object.Err != nil {
			fmt.Println("Error detected during files listing:", object.Err)
			continue
		}
		files = append(files, object)
	}

	sort.Sort(files)

	files = files[saveLast:]

	toClean := make(chan string, len(files))

	for _, file := range files {
		toClean <- file.Key
	}
	close(toClean)
	return toClean, nil
}

type files []minio.ObjectInfo

func (f files) Len() int {
	return len(f)
}

func (f files) Less(i, j int) bool {
	return f[i].LastModified.Before(f[j].LastModified)
}

func (f files) Swap(i, j int) {
	f[i], f[j] = f[j], f[i]
}
