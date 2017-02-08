package main

// Copyright (c) 2016 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

// Tool receives raw events from go-couchbase UPR client.


import (
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/couchbase/go-couchbase"
	mc "github.com/couchbase/gomemcached"
	mcc "github.com/couchbase/gomemcached/client"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/base"
	parts "github.com/couchbase/goxdcr/parts"
	utils "github.com/couchbase/goxdcr/utils"
	"net/http"
	"os"
	"sync"
	"time"
)

import _ "net/http/pprof"

var logger *log.CommonLogger = log.NewLogger("Messaging_run", log.DefaultLoggerContext)


var options struct {
	source_bucket       string // source bucket
	target_bucket       string //target bucket
	source_cluster_addr string //source connect string
	target_cluster_addr string //target connect string
	username            string //username
	password            string //password
	maxVbno             int    // maximum number of vbuckets
}

var done = make(chan bool, 16)
var rch = make(chan []interface{}, 10000)
var uprFeed *couchbase.UprFeed = nil
var msg *parts.MessagingNozzle = nil
var target_bk *couchbase.Bucket

func argParse() {
	flag.StringVar(&options.source_cluster_addr, "source_cluster_addr", "127.0.0.1:9000",
		"source cluster address")
	flag.StringVar(&options.target_cluster_addr, "target_cluster_addr", "127.0.0.1:9000",
		"target cluster address")
	flag.StringVar(&options.source_bucket, "source_bucket", "travel-sample",
		"bucket to replicate from")
	flag.IntVar(&options.maxVbno, "maxvb", 1024,
		"maximum number of vbuckets")
	//flag.StringVar(&options.target_bucket, "target_bucket", "target",
		//"bucket to replicate to")
	flag.StringVar(&options.username, "username", "Administrator","username")
	flag.StringVar(&options.password, "password", "welcome","password")

	flag.Parse()
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage : %s [OPTIONS] \n", os.Args[0])
	flag.PrintDefaults()
}

func setup() (err error) {

	logger.Info("Start Testing Messaging...")
	logger.Infof("target_clusterAddr=%s, username=%s, password=%s\n", options.target_cluster_addr, options.username, options.password)
	logger.Info("Done with parsing the arguments")
	return
}


func main() {
	//start http server for pprof
	go func() {
		logger.Info("Try to start pprof...")
		err := http.ListenAndServe("localhost:7000", nil)
		if err != nil {
			panic(err)
		} else {
			logger.Info("Http server for pprof is started")
		}
	}()
	argParse()

	test(500, 1000)
	//	test(5, 50, parts.Batch_XMEM)

}

func verify(data_count int) bool {
	
	return true
}

func test(batch_count int, data_count int) {
	
	logger.Info("------------START testing Messaging-------------------")
	logger.Infof("batch_count=%d, data_count=%d\n", batch_count, data_count)
	
	startMessaging(batch_count)
	logger.Info("Messaging is started")
	waitGrp := &sync.WaitGroup{}
	waitGrp.Add(1)
	go startUpr(options.source_cluster_addr, options.source_bucket, waitGrp, data_count)
	waitGrp.Wait()

	time.Sleep(100 * time.Second)
	bSuccess := verify(data_count)
	if bSuccess {
		logger.Info("----------TEST SUCCEED------------")
	} else {
		logger.Info("----------TEST FAILED-------------")
	}
}

func  composeMCRequest(event *mcc.UprEvent) (*base.WrappedMCRequest, error) {
	wrapped_req := &base.WrappedMCRequest{Seqno: 0, Req: &mc.MCRequest{},}

	req := wrapped_req.Req
	req.Cas = event.Cas
	req.Opaque = 0
	req.VBucket = event.VBucket
	req.Key = event.Key
	req.Body = event.Value
	//opCode
	req.Opcode = event.Opcode

	//extra
	if event.Opcode == mc.UPR_MUTATION || event.Opcode == mc.UPR_DELETION ||
		event.Opcode == mc.UPR_EXPIRATION {
		if len(req.Extras) != 24 {
			req.Extras = make([]byte, 24)
		}
		//    <<Flg:32, Exp:32, SeqNo:64, CASPart:64, 0:32>>.
		binary.BigEndian.PutUint32(req.Extras[0:4], event.Flags)
		binary.BigEndian.PutUint32(req.Extras[4:8], event.Expiry)
		binary.BigEndian.PutUint64(req.Extras[8:16], event.RevSeqno)
		binary.BigEndian.PutUint64(req.Extras[16:24], event.Cas)

	} else if event.Opcode == mc.UPR_SNAPSHOT {
		if len(req.Extras) != 28 {
			req.Extras = make([]byte, 28)
		}
		binary.BigEndian.PutUint64(req.Extras[0:8], event.Seqno)
		binary.BigEndian.PutUint64(req.Extras[8:16], event.SnapstartSeq)
		binary.BigEndian.PutUint64(req.Extras[16:24], event.SnapendSeq)
		binary.BigEndian.PutUint32(req.Extras[24:28], event.SnapshotType)
	}

	wrapped_req.Seqno = event.Seqno
	wrapped_req.Start_time = time.Now()
	wrapped_req.ConstructUniqueKey()

	return wrapped_req, nil
}

func startUpr(cluster, bucketn string, waitGrp *sync.WaitGroup, data_count int) {
	logger.Info("Start Upr...")
	b, err := utils.LocalBucket(cluster, bucketn)
	mf(err, "bucket")

	uprFeed, err = b.StartUprFeedWithConfig("rawupr", uint32(0), 1000, 1024*1024)
	mf(err, "- upr")

	flogs := failoverLogs(b)
	logger.Info("Got failover log successfully")

	// list of vbuckets
	vbnos := make([]uint16, 0, options.maxVbno)
	for i := 0; i < options.maxVbno; i++ {
		vbnos = append(vbnos, uint16(i))
	}

	startStream(uprFeed, flogs)
	logger.Info("Upr stream is started")

	count := 0
	for {
		e, ok := <-uprFeed.C
		if ok == false {
			logger.Infof("Closing for bucket %v\n", b.Name)
		}

		//transfer UprEvent to MCRequest
		switch e.Opcode {
		case mc.UPR_MUTATION, mc.UPR_DELETION, mc.UPR_EXPIRATION:
			mcReq , err := composeMCRequest(e)
			count++
			logger.Infof("Number of upr event received so far is %d\n", count)
            if err == nil {
			    msg.Receive(mcReq)
			} else {
					logger.Infof("Error in wrapping MC Request is %v\n", err)
			}
            
		}
		if count >= data_count {
			goto Done
		}

	}
Done:
	//close the upr stream
	logger.Info("Stoping DCP Stream Done.........")
	uprFeed.Close()
	msg.Stop()
	waitGrp.Done()
}

func startStream(uprFeed *couchbase.UprFeed, flogs couchbase.FailoverLog) {
	start, end := uint64(0), uint64(0xFFFFFFFFFFFFFFFF)
	snapStart, snapEnd := uint64(0), uint64(0)
	for vbno, flog := range flogs {
		x := flog[len(flog)-1] // map[uint16][][2]uint64
		flags, vbuuid := uint32(0), x[0]
		err := uprFeed.UprRequestStream(
			vbno, vbno, flags, vbuuid, start, end, snapStart, snapEnd)
		mf(err, fmt.Sprintf("stream-req for %v failed", vbno))
	}
}

func failoverLogs(b *couchbase.Bucket) couchbase.FailoverLog {
	// list of vbuckets
	vbnos := make([]uint16, 0, options.maxVbno)
	for i := 0; i < options.maxVbno; i++ {
		vbnos = append(vbnos, uint16(i))
	}

	flogs, err := b.GetFailoverLogs(vbnos)
	mf(err, "- upr failoverlogs")
	return flogs
}

func mf(err error, msg string) {
	if err != nil {
		logger.Errorf("%v: %v", msg, err)
	}
}



func startMessaging(batch_count int) {
	//target_connectStr, err := getConnectStr(options.target_cluster_addr, "default", options.target_bucket, options.username, options.password)
	//if err != nil || target_connectStr == "" {
		//panic(err)
	//}
	//logger.Infof("target_connectStr=%s\n", target_connectStr)
	certificate := make([]byte, 0)
    msg = parts.NewMessagingNozzle("msgNozzle", "test_msg", "test_msg", options.username, options.password,certificate, logger.LoggerContext())
	var configs map[string]interface{} = map[string]interface{}{parts.SETTING_BATCHCOUNT: batch_count,
		parts.SETTING_RESP_TIMEOUT: time.Millisecond * 10,
		parts.SETTING_NUMOFRETRY:   3}

	msg.Start(configs)
}


