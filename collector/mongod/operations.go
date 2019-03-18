package mongod

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"time"
)

var (
	longestQuery = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "operations",
		Name:      "longest_query_seconds",
		Help:      "Longest query running time in seconds",
	})

	indexBuilding = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "operations",
		Name:      "index_building",
		Help:      "Is index building query running, 0 - no, 1 - yes",
	})

	indexBuildingProgress = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: Namespace,
		Subsystem: "operations",
		Name:      "index_building_progress",
		Help:      "Holds count of building index total and done records",
	}, []string{"records"})
)

// Operation struct to unmarshal operations in mongod
type Operation struct {
	Op               string `bson:"op"`
	Ns               string `bson:"ns"`
	SecsRunning      int64  `bson:"secs_running"`
	MicroSecsRunning int64  `bson:"microsecs_running"`
	Msg              string `bson:"msg"`
	Progess          struct {
		Done  int32 `bson:"done"`
		Total int32 `bson:"total"`
	} `bson:"progress"`
}

// OperationsList struct to hold response from currentOp() mongod call
type OperationsList struct {
	Inprog []Operation `bson:"inprog"`
}

// OperationsStatus holds results for metrics
type OperationsStatus struct {
	LongestQueryTime           time.Duration
	IndexBuilding              bool
	IndexBuildingProgressTotal int32
	IndexBuildingProgressDone  int32
}

// Export exports database stats to prometheus
func (o *OperationsStatus) Export(ch chan<- prometheus.Metric) {
	// reset previously collected values
	longestQuery.Set(o.LongestQueryTime.Seconds())
	longestQuery.Collect(ch)
	var index float64

	if o.IndexBuilding {
		index = 1

		indexBuildingProgress.WithLabelValues("total").Set(float64(o.IndexBuildingProgressTotal))
		indexBuildingProgress.WithLabelValues("done").Set(float64(o.IndexBuildingProgressDone))
		indexBuildingProgress.Collect(ch)
	}

	indexBuilding.Set(index)
	indexBuilding.Collect(ch)
}

// Describe describes database stats for prometheus
func (o *OperationsStatus) Describe(ch chan<- *prometheus.Desc) {
	longestQuery.Describe(ch)
	indexBuilding.Describe(ch)
	indexBuildingProgress.Describe(ch)
}

// GetOperationsStatus returns time of longest running query/command and boolean for index is building or not
func GetOperationsStatus(session *mgo.Session) *OperationsStatus {
	operationsList := &OperationsList{}
	var longestQueryDuration time.Duration
	err := session.DB("admin").Run(bson.D{{"currentOp", 1}, {"$all", true}, {"op", bson.M{"$in": []string{"command", "query", "update", "delete", "insert", "getmore"}}}}, &operationsList)
	if err != nil {
		log.Error("Failed to get current operations list")
		return nil
	}

	for _, op := range operationsList.Inprog {
		// skip replication getmore queries
		if op.Op == "getmore" && op.Ns=="local.oplog.rs" {
			continue
		}

		optime := time.Duration(op.SecsRunning)*time.Second + time.Duration(op.MicroSecsRunning)*time.Microsecond

		if optime > longestQueryDuration {
			longestQueryDuration = optime
		}
	}

	result := &OperationsStatus{}
	result.LongestQueryTime = longestQueryDuration

	check1 := bson.M{"op": "command", "query.createIndexes": bson.M{"$exists": true}}
	check2 := bson.M{"op": "insert", "ns": bson.RegEx{`.\.system\.indexes`, "s"}}
	check3 := bson.M{"msg": bson.RegEx{"Index Build.*", "s"}}

	err = session.DB("admin").Run(bson.D{{"currentOp", 1}, {"$or", []interface{}{check1, check2, check3}}}, &operationsList)
	if err != nil {
		log.Error("Failed to get current operations list for index build check")
		return nil
	}

	if len(operationsList.Inprog) > 0 { // Report only first encountered index build
		result.IndexBuildingProgressTotal = operationsList.Inprog[0].Progess.Total
		result.IndexBuildingProgressDone = operationsList.Inprog[0].Progess.Done
	}

	result.IndexBuilding = len(operationsList.Inprog) > 0
	return result
}
