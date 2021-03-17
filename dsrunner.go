// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package dataflow contains the Dataflow runner for submitting pipelines
// to Google Cloud Dataflow.
package dsrunner

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
  "flag"
  "os"

  "path"
  "runtime"

	"github.com/deanmao/dsrunner/dsrunnerlib"
	"github.com/apache/beam/sdks/go/pkg/beam"
	_ "github.com/apache/beam/sdks/go/pkg/beam/runners/direct"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime/graphx"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/universal/runnerlib"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/hooks"
	"github.com/apache/beam/sdks/go/pkg/beam/util/gcsx"
	"github.com/golang/protobuf/proto"
)

var (
  createBinary = flag.Bool("create_binary", false, "generate binary (optional).")
)

type Options struct {
	JobName             string
	Project             string
  Region              string
  Subnetwork          string
  NumWorkers          int64
	MachineType         string
	ServiceAccountEmail string
	MaxNumWorkers       int64
  TempLocation        string
	StagingLocation     string
  DryRun              bool
  Async               bool
  CreateBinary        bool

	// Worker is the worker binary override.
	Worker string
}
var options Options

func Init(opts Options) {
  options = opts
	beam.RegisterRunner("dsrunner", Execute)
}

var unique int32
func GetJobName(prefix string) string {
	return fmt.Sprintf("%s-%v", prefix, time.Now().UnixNano())
}

func getContainerImage(ctx context.Context) string {
  return "apache/beam_go_sdk:latest"
}

// Execute runs the given pipeline on Google Cloud Dataflow. It uses the
// default application credentials to submit the job.
func Execute(ctx context.Context, p *beam.Pipeline) (beam.PipelineResult, error) {
	// (1) Gather job options

  stagingLocation := options.StagingLocation
	project := options.Project
	if project == "" {
		panic("no Google Cloud project specified. Use --project=<project>")
	}
	region := options.Region
	if region == "" {
		panic("No Google Cloud region specified. Use --region=<region>. See https://cloud.google.com/dataflow/docs/concepts/regional-endpoints")
	}
	if stagingLocation == "" {
		panic("no GCS staging location specified. Use --staging_location=gs://<bucket>/<path>")
	}
	var jobLabels map[string]string
	hooks.SerializeHooksToOptions()

	opts := &dsrunnerlib.JobOptions{
		Name:                GetJobName(options.JobName),
		Experiments:         []string{"use_unified_worker"},
		Options:             beam.PipelineOptions.Export(),
		Project:             project,
		Region:              region,
		Zone:                "",
		Network:             "",
		Subnetwork:          options.Subnetwork,
		NoUsePublicIPs:      false,
		NumWorkers:          options.NumWorkers,
		MaxNumWorkers:       options.MaxNumWorkers,
		DiskSizeGb:          0,
		Algorithm:           "",
		MachineType:         options.MachineType,
		Labels:              jobLabels,
		ServiceAccountEmail: options.ServiceAccountEmail,
		TempLocation:        options.TempLocation,
		Worker:              options.Worker,
		WorkerJar:           "",
		WorkerRegion:        "",
		WorkerZone:          "",
		TeardownPolicy:      "",
	}
	if opts.TempLocation == "" {
		opts.TempLocation = gcsx.Join(stagingLocation, "tmp")
	}

	// (1) Build and submit

	edges, _, err := p.Build()
	if err != nil {
		return nil, err
	}
	enviroment, err := graphx.CreateEnvironment(ctx, "beam:env:docker:v1", getContainerImage)
	if err != nil {
    panic(err)
	}
	model, err := graphx.Marshal(edges, &graphx.Options{Environment: enviroment})
	if err != nil {
    panic(err)
	}

	id := fmt.Sprintf("go-%v-%v", atomic.AddInt32(&unique, 1), time.Now().UnixNano())
	modelURL := gcsx.Join(stagingLocation, id, "model")
	workerURL := gcsx.Join(stagingLocation, id, "worker")
	jarURL := gcsx.Join(stagingLocation, id, "dataflow-worker.jar")

	if options.DryRun {
		fmt.Println("Dry-run: not submitting job!")

		fmt.Println(proto.MarshalTextString(model))
		job, err := dsrunnerlib.Translate(ctx, model, opts, workerURL, jarURL, modelURL)
		if err != nil {
			return nil, err
		}
		dsrunnerlib.PrintJob(ctx, job)
		return nil, nil
	}
  if *createBinary == true {
    _, filename, _, ok := runtime.Caller(0)
    if !ok {
        panic("No caller information")
    }
    binariesDir := path.Dir(filename) + "/dsrunnerlib/binaries"
    os.Chmod(binariesDir, 0755)
    binaryFilePath := binariesDir + "/worker_bin"
		worker, err := runnerlib.BuildTempWorkerBinary(ctx)
		if err != nil {
			panic(err)
		}
    err2 := os.Rename(worker, binaryFilePath)
    if err2 != nil {
      panic(err2)
    }
    fmt.Println("Binary created")
    return nil, nil
  }

	return dsrunnerlib.Execute(ctx, model, opts, workerURL, jarURL, modelURL, "", options.Async)
}

func Run(p *beam.Pipeline) error {
  _, err := beam.Run(context.Background(), "dsrunner", p)
  return err
}

func RunLocally(p *beam.Pipeline) error {
  _, err := beam.Run(context.Background(), "direct", p)
  return err
}
