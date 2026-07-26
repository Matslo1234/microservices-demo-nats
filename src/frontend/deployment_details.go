package main

import (
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"cloud.google.com/go/compute/metadata"
	"github.com/sirupsen/logrus"
)

var deploymentDetails atomic.Value
var log *logrus.Logger

func init() {
	initializeLogger()
	deploymentDetails.Store(map[string]string{})
	// Use a goroutine to ensure loadDeploymentDetails()'s GCP API
	// calls don't block non-GCP deployments. See issue #685.
	go loadDeploymentDetails()
}

func initializeLogger() {
	log = logrus.New()
	log.Level = logrus.DebugLevel
	log.Formatter = &logrus.JSONFormatter{
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyTime:  "timestamp",
			logrus.FieldKeyLevel: "severity",
			logrus.FieldKeyMsg:   "message",
		},
		TimestampFormat: time.RFC3339Nano,
	}
	log.Out = os.Stdout
}

func loadDeploymentDetails() {
	var metaServerClient = metadata.NewClient(&http.Client{})

	podHostname, err := os.Hostname()
	if err != nil {
		log.Error("Failed to fetch the hostname for the Pod", err)
	}

	podCluster, err := metaServerClient.InstanceAttributeValue("cluster-name")
	if err != nil {
		log.Error("Failed to fetch the name of the cluster in which the pod is running", err)
	}

	podZone, err := metaServerClient.Zone()
	if err != nil {
		log.Error("Failed to fetch the Zone of the node where the pod is scheduled", err)
	}

	deploymentDetails.Store(map[string]string{
		"HOSTNAME":    podHostname,
		"CLUSTERNAME": podCluster,
		"ZONE":        podZone,
	})

	log.WithFields(logrus.Fields{
		"cluster":  podCluster,
		"zone":     podZone,
		"hostname": podHostname,
	}).Debug("Loaded deployment details")
}

func currentDeploymentDetails() map[string]string {
	return deploymentDetails.Load().(map[string]string)
}
