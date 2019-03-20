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

package cmd

import (
	"fmt"
	"github.com/Percona-Lab/s3backupcleaner/cleaner"
	"github.com/pkg/errors"
	"net/url"
	"os"

	"github.com/spf13/cobra"
)

var (
	key    string
	secret string
	ssl    bool

	endpoint string

	bucket    string
	bucketUrl url.URL
	saveLast  int
)

// rootCmd represents the base command when called without any subcommands.
var rootCmd = &cobra.Command{
	Use:   "s3bc",
	Short: "",
}

// cleanCmd represents the clean command which contain the logic of deletion the old backups from S3 API like storage's.
var cleanCmd = &cobra.Command{
	Use:   "clean",
	Short: "Clean bucket from old backups",

	PreRunE: func(cmd *cobra.Command, args []string) error {
		if key == "" {
			return errors.New("key flag or AWS_ACCESS_KEY_ID environment variable must be specified")
		}
		if secret == "" {
			return errors.New("secret flag or AWS_SECRET_ACCESS_KEY environment variable must be specified")
		}
		if endpoint == "" {
			return errors.New("endpoint flag or AWS_ENDPOINT_URL environment variable must be specified")
		}
		if bucket == "" {
			return errors.New("bucket flag must be specified")
		}
		url, err := parseBucket(bucket)
		if err != nil {
			return err
		}
		bucketUrl = *url

		return nil
	},

	RunE: func(cmd *cobra.Command, args []string) error {
		creds := cleaner.Credentials{
			Key:    key,
			Secret: secret,
			Secure: ssl,
		}

		clean, err := cleaner.NewCleaner(endpoint, creds)
		if err != nil {
			return errors.Wrap(err, "can't create cleaner")
		}
		if err := clean.Clean(bucketUrl, saveLast); err != nil {
			return errors.Wrap(err, "cleaning has failed")
		}

		return nil
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func parseBucket(bucketUrl string) (*url.URL, error) {
	u, err := url.Parse(bucketUrl)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse s3 URL")
	}
	if u.Scheme != "s3" {
		return nil, errors.New("url should starts with s3://")
	}
	return u, nil
}

func init() {
	rootCmd.AddCommand(cleanCmd)
	rootCmd.PersistentFlags().StringVar(&key,
		"key",
		os.Getenv("AWS_ACCESS_KEY_ID"),
		"set the S3 access key",
	)
	rootCmd.PersistentFlags().StringVar(&secret,
		"secret",
		os.Getenv("AWS_SECRET_ACCESS_KEY"),
		"set the S3 secret key",
	)
	rootCmd.PersistentFlags().BoolVar(&ssl,
		"ssl",
		false,
		"set HTTPS for api calls",
	)
	rootCmd.PersistentFlags().StringVar(&endpoint,
		"endpoint",
		os.Getenv("AWS_ENDPOINT_URL"),
		"set the S3 endpoint",
	)
	cleanCmd.Flags().StringVar(&bucket,
		"bucket",
		"",
		"set bucket path in format s3://bucket or s3://bucket/folder",
	)
	cleanCmd.Flags().IntVar(&saveLast,
		"save-last", 5,
		"set number of backups which should be kept",
	)
}
