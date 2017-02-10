// Copyright 2012-2016 Apcera Inc. All rights reserved.
// +build ignore

package main

import (
	"flag"
	"fmt"
	"log"
        "strconv"
	"os"
	"os/signal"
	"time"
         "encoding/json" 
        b64 "encoding/base64"
	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/go-nats-streaming/pb"
        elastic "gopkg.in/olivere/elastic.v2" 
)

var usageStr = `
Usage: stan-sub [options] <subject>

Options:
	-s, --server   <url>            NATS Streaming server URL(s)
	-c, --cluster  <cluster name>   NATS Streaming cluster name
	-id,--clientid <client ID>      NATS Streaming client ID

Subscription Options:
	--qgroup <name>                 Queue group
	--seq <seqno>                   Start at seqno
	--all                           Deliver all available messages
	--last                          Deliver starting with last published message
	--since <duration>              Deliver messages in last interval (e.g. 1s, 1hr)
	         (for more information: https://golang.org/pkg/time/#ParseDuration)
	--durable <name>                Durable subscriber name
	--unsubscribe                   Unsubscribe the durable on exit
`

// NOTE: Use tls scheme for TLS, e.g. stan-sub -s tls://demo.nats.io:4443 foo
func usage() {
	log.Fatalf(usageStr)
}

var client *elastic.Client

func getESClient()  {
  // Obtain a client and connect to the default Elasticsearch installation
  // on 127.0.0.1:9200. Of course you can configure your client to connect
  // to other hosts and configure it in various other ways.
  lclient, err := elastic.NewClient(elastic.SetURL("http://172.23.107.55:9200"))
  if err != nil {
    // Handle error
    panic(err)
  }

  // Ping the Elasticsearch server to get e.g. the version number
  //info, code, err := client.Ping().Do()
 // if err != nil {
    // Handle error
  //  panic(err)
 // }
 // fmt.Printf("Elasticsearch returned with code %d and version %s", code, info.Version.Number)

  // Getting the ES version number is quite common, so there's a shortcut
  esversion, err := lclient.ElasticsearchVersion("http://172.23.107.55:9200")
  if err != nil {
    // Handle error
    panic(err)
  }
  fmt.Printf("Elasticsearch version %s", esversion) 
  client = lclient
}


func createTravelIndex() {

  // Use the IndexExists service to check if a specified index exists.
  exists, err := client.IndexExists("travel3").Do()
  if err != nil {
    // Handle error
    panic(err)
  }
  if !exists {
    // Create a new index.
    createIndex, err := client.CreateIndex("travel3").Do()
    if err != nil {
      // Handle error
      panic(err)
    }
    if !createIndex.Acknowledged {
      // Not acknowledged
    }
  }

}



func indexTravelDoc(tDoc Travel) {
put1, err := client.Index().
   Index("travel3").
    Type("trav").
    Id(strconv.Itoa(tDoc.Id)).
    BodyJson(tDoc).
    Do()
  if err != nil {
    // Handle error
    panic(err)
  }
  fmt.Printf("Indexed travel document %s to index %s, type %s\n", put1.Id, put1.Index, put1.Type)
}

type Travel struct {
         Airline            string `json:"airline"`
	Airlineid          string `json:"airlineid"`
	Destinationairport string `json:"destinationairport"`
	Sourceairport      string `json:"sourceairport"` 
	Callsign string `json:"callsign"`

	Country string `json:"country"`

	Iata string `json:"iata"`

	Icao string `json:"icao"`

	Id int `json:"id"`

	Name string `json:"name"`

	Type string `json:"type"`
}

func printMsg(m *stan.Msg, i int) {
          doc_map := make(map[string]interface{})
	_ = json.Unmarshal(m.Data, &doc_map)
	//log.Printf("[#%d] Received on [%s]: '%s'\n", i, m.Subject, doc_map["base64"])
         sDec, _ := b64.StdEncoding.DecodeString(doc_map["base64"].(string)) 
	log.Printf("[#%d] Received on [%s]: '%s'\n", i, m.Subject, sDec)
         byt := []byte(sDec)
        var app Travel 
        err := json.Unmarshal(byt, &app)
	if err == nil {
                fmt.Println(strconv.Itoa(app.Id)) 
                fmt.Println(app.Airline) 
		fmt.Println(app.Airlineid)
		fmt.Println(app.Name)
		fmt.Println(app.Type)
		fmt.Println(app.Destinationairport)
		fmt.Println(app.Sourceairport)
	}
    //index document
     indexTravelDoc(app) 
}

func main() {
	var clusterID string
	var clientID string
	var showTime bool
	var startSeq uint64
	var startDelta string
	var deliverAll bool
	var deliverLast bool
	var durable string
	var qgroup string
	var unsubscribe bool
	var URL string

	//	defaultID := fmt.Sprintf("client.%s", nuid.Next())

	flag.StringVar(&URL, "s", stan.DefaultNatsURL, "The nats server URLs (separated by comma)")
	flag.StringVar(&URL, "server", stan.DefaultNatsURL, "The nats server URLs (separated by comma)")
	flag.StringVar(&clusterID, "c", "test-cluster", "The NATS Streaming cluster ID")
	flag.StringVar(&clusterID, "cluster", "test-cluster", "The NATS Streaming cluster ID")
	flag.StringVar(&clientID, "id", "", "The NATS Streaming client ID to connect with")
	flag.StringVar(&clientID, "clientid", "", "The NATS Streaming client ID to connect with")
	flag.BoolVar(&showTime, "t", false, "Display timestamps")
	// Subscription options
	flag.Uint64Var(&startSeq, "seq", 0, "Start at sequence no.")
	flag.BoolVar(&deliverAll, "all", false, "Deliver all")
	flag.BoolVar(&deliverLast, "last", false, "Start with last value")
	flag.StringVar(&startDelta, "since", "", "Deliver messages since specified time offset")
	flag.StringVar(&durable, "durable", "", "Durable subscriber name")
	flag.StringVar(&qgroup, "qgroup", "", "Queue group name")
	flag.BoolVar(&unsubscribe, "unsubscribe", false, "Unsubscribe the durable on exit")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()

	if clientID == "" {
		log.Printf("Error: A unique client ID must be specified.")
		usage()
	}
	if len(args) < 1 {
		log.Printf("Error: A subject must be specified.")
		usage()
	}

       /// connect to elastic search and create Travel Index
         getESClient()
         createTravelIndex() 

	sc, err := stan.Connect(clusterID, clientID, stan.NatsURL(URL))
	if err != nil {
		log.Fatalf("Can't connect: %v.\nMake sure a NATS Streaming Server is running at: %s", err, URL)
	}
	log.Printf("Connected to %s clusterID: [%s] clientID: [%s]\n", URL, clusterID, clientID)

	subj, i := args[0], 0

	mcb := func( msg *stan.Msg) {
		i++
		printMsg(msg, i)
              // Flush to make sure the documents got written.
              _, err = client.Flush().Index("travel3").Do()
              if err != nil {
                   panic(err)
                } 
	}

	startOpt := stan.StartAt(pb.StartPosition_NewOnly)

	if startSeq != 0 {
		startOpt = stan.StartAtSequence(startSeq)
	} else if deliverLast == true {
		startOpt = stan.StartWithLastReceived()
	} else if deliverAll == true {
		log.Print("subscribing with DeliverAllAvailable")
		startOpt = stan.DeliverAllAvailable()
	} else if startDelta != "" {
		ago, err := time.ParseDuration(startDelta)
		if err != nil {
			sc.Close()
			log.Fatal(err)
		}
		startOpt = stan.StartAtTimeDelta(ago)
	}

	sub, err := sc.QueueSubscribe(subj, qgroup, mcb, startOpt, stan.DurableName(durable))
	if err != nil {
		sc.Close()
		log.Fatal(err)
	}

	log.Printf("Listening on [%s], clientID=[%s], qgroup=[%s] durable=[%s]\n", subj, clientID, qgroup, durable)

	if showTime {
		log.SetFlags(log.LstdFlags)
	}

	// Wait for a SIGINT (perhaps triggered by user with CTRL-C)
	// Run cleanup when signal is received
	signalChan := make(chan os.Signal, 1)
	cleanupDone := make(chan bool)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		for _ = range signalChan {
			fmt.Printf("\nReceived an interrupt, unsubscribing and closing connection...\n\n")
			// Do not unsubscribe a durable on exit, except if asked to.
			if durable == "" || unsubscribe {
				sub.Unsubscribe()
			}
			sc.Close()
			cleanupDone <- true
		}
	}()
	<-cleanupDone
}
