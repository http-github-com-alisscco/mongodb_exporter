// mongodb_exporter
// Copyright (C) 2017 Percona LLC
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

package exporter

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// This collector is fetching current running ops and provides metric of longest running query, or indexes being built.
type operationsCollector struct {
	ctx            context.Context
	client         *mongo.Client
	compatibleMode bool
	logger         *logrus.Logger
	topologyInfo   labelsGetter
}

func (d *operationsCollector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(d, ch)
}

func (d *operationsCollector) Collect(ch chan<- prometheus.Metric) {
	statuses := getOperationsStatus(d.ctx, d.client, d.logger)

	if statuses == nil {
		return
	}

	doc := bson.M{"operations": bson.M{
		"longest_running_query_seconds": float64(statuses.LongestQueryTime) / float64(time.Second),
		"index_building":                statuses.IndexBuilding,
		"index_building_progress": bson.M{
			"done":  statuses.IndexBuildingProgressDone,
			"total": statuses.IndexBuildingProgressTotal,
		},
	}}

	for _, metric := range makeMetrics("", doc, d.topologyInfo.baseLabels(), d.compatibleMode) {
		ch <- metric
	}
}

// operation struct to unmarshal operations in mongod.
type operation struct {
	Op               string `bson:"op"`
	Ns               string `bson:"ns"`
	SecsRunning      int64  `bson:"secs_running"`
	MicroSecsRunning int64  `bson:"microsecs_running"`
	Msg              string `bson:"msg"`
	Active           bool   `bson:"active"`
	Progess          struct {
		Done  int32 `bson:"done"`
		Total int32 `bson:"total"`
	} `bson:"progress"`
}

// operationsList struct to hold response from currentOp() mongod call.
type operationsList struct {
	Inprog []operation `bson:"inprog"`
}

// operationsStatus holds results for metrics.
type operationsStatus struct {
	LongestQueryTime           time.Duration
	IndexBuilding              bool
	IndexBuildingProgressTotal int32
	IndexBuildingProgressDone  int32
}

// getOperationsStatus returns time of longest running query/command and boolean for index is building or not.
func getOperationsStatus(ctx context.Context, client *mongo.Client, log *logrus.Logger) *operationsStatus {
	operationsList := &operationsList{}
	var longestQueryDuration time.Duration

	oplistQuery := bson.D{
		{Key: "currentOp", Value: 1},
		{Key: "op", Value: bson.M{"$in": []string{"command", "query", "update", "delete", "insert"}}},
	}

	singleResult := client.Database("admin").RunCommand(ctx, oplistQuery)
	if singleResult.Err() != nil {
		log.Errorf("failed to get current operations list: %w", singleResult.Err())
		return nil
	}

	err := singleResult.Decode(&operationsList)
	if err != nil {
		log.Errorf("failed to decode current operations list: %w", err)
	}

	log.Debugf("ops running:")
	debugResult(log, operationsList)

	for _, op := range operationsList.Inprog {
		optime := time.Duration(op.MicroSecsRunning) * time.Microsecond
		if op.Active && optime > longestQueryDuration {
			longestQueryDuration = optime
		}
	}

	result := &operationsStatus{LongestQueryTime: longestQueryDuration}
	check1 := bson.M{"op": "command", "query.createIndexes": bson.M{"$exists": true}}
	check2 := bson.M{"op": "insert", "ns": primitive.Regex{Pattern: `.\.system\.indexes`, Options: "s"}}
	check3 := bson.M{"msg": primitive.Regex{Pattern: "Index Build.*", Options: "s"}}

	finalQuery := bson.D{{Key: "currentOp", Value: 1}, {Key: "$or", Value: []interface{}{check1, check2, check3}}}

	singleResult = client.Database("admin").RunCommand(ctx, finalQuery)
	if singleResult.Err() != nil {
		log.Errorf("failed to get current operations list for index build check: %w", singleResult.Err())
		return nil
	}

	err = singleResult.Decode(operationsList)
	if err != nil {
		log.Errorf("failed to decode index build operations list: %w", err)
		return nil
	}

	if len(operationsList.Inprog) > 0 { // Report only first encountered index build
		result.IndexBuildingProgressTotal = operationsList.Inprog[0].Progess.Total
		result.IndexBuildingProgressDone = operationsList.Inprog[0].Progess.Done
	}

	result.IndexBuilding = len(operationsList.Inprog) > 0
	return result
}

var _ prometheus.Collector = (*operationsCollector)(nil)
