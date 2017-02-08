package parts

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	base "github.com/couchbase/goxdcr/base"
	common "github.com/couchbase/goxdcr/common"
	gen_server "github.com/couchbase/goxdcr/gen_server"
	mc "github.com/couchbase/gomemcached"
	"github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/utils"
	"github.com/couchbase/goxdcr/simple_utils"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
	"flag"
	"github.com/nats-io/go-nats-streaming"
)


const (

	MSG_SETTING_CONNECTION_TIMEOUT = "connection_timeout"
	MSG_SETTING_RETRY_INTERVAL     = "retry_interval"

	//default configuration
	default_msg_numofretry_messaging          int           = 6
	default_msg_retry_interval_messaging      time.Duration = 30 * time.Millisecond
	default_msg_maxRetryInterval_messaging                  = 30 * time.Second
	default_msg_writeTimeout_messaging        time.Duration = time.Duration(1) * time.Second
	default_msg_readTimeout_messaging         time.Duration = time.Duration(10) * time.Second
	default_msg_connection_timeout                     = 180 * time.Second 
	default_msg_selfMonitorInterval_messaging time.Duration = 300 * time.Millisecond

)

var messaging_setting_defs base.SettingDefinitions = base.SettingDefinitions{SETTING_BATCHCOUNT: base.NewSettingDef(reflect.TypeOf((*int)(nil)), true),
	SETTING_BATCHSIZE:             base.NewSettingDef(reflect.TypeOf((*int)(nil)), true),
	SETTING_OPTI_REP_THRESHOLD:    base.NewSettingDef(reflect.TypeOf((*int)(nil)), true),
	SETTING_BATCH_EXPIRATION_TIME: base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false),
	SETTING_NUMOFRETRY:            base.NewSettingDef(reflect.TypeOf((*int)(nil)), false),
	SETTING_RETRY_INTERVAL:        base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false),
	SETTING_WRITE_TIMEOUT:         base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false),
	SETTING_READ_TIMEOUT:          base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false),
	SETTING_MAX_RETRY_INTERVAL:    base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false),
	SETTING_CONNECTION_TIMEOUT:    base.NewSettingDef(reflect.TypeOf((*time.Duration)(nil)), false)}


/************************************
/* struct messagingBatch
*************************************/
type messagingBatch struct {
	dataBatch
}


/************************************
/* struct Config
*************************************/
type messagingConfig struct {
	baseConfig
	connectionTimeout time.Duration
	retryInterval     time.Duration
	bucketName string
	respTimeout        unsafe.Pointer // *time.Duration
	max_read_downtime  time.Duration
	logger             *log.CommonLogger
}

func newMessagingConfig(logger *log.CommonLogger) messagingConfig {
	return messagingConfig{
		baseConfig: baseConfig{maxCount: -1,
			maxSize:             -1,
			maxRetry:            default_msg_numofretry_messaging,
			writeTimeout:        default_msg_writeTimeout_messaging,
			readTimeout:         default_msg_readTimeout_messaging,
			maxRetryInterval:    default_msg_maxRetryInterval_messaging,
			selfMonitorInterval: default_msg_selfMonitorInterval_messaging,
			connectStr:          "",
			username:            "",
			password:            "",
		},
		connectionTimeout: default_msg_connection_timeout,
		retryInterval:     default_msg_retry_interval_messaging,
	}

}

func (config *messagingConfig) initializeConfig(settings map[string]interface{}) error {
	err := utils.ValidateSettings(messaging_setting_defs, settings, config.logger)

	if err == nil {
		config.baseConfig.initializeConfig(settings)

		if val, ok := settings[SETTING_CONNECTION_TIMEOUT]; ok {
			config.connectionTimeout = val.(time.Duration)
		}
		if val, ok := settings[SETTING_RETRY_INTERVAL]; ok {
			config.retryInterval = val.(time.Duration)
		}
	}
	return err
}

/************************************
/* struct MessagingNozzle
*************************************/
type MessagingNozzle struct {
	//parent inheritance
	gen_server.GenServer
	AbstractPart

	bOpen      bool
	lock_bOpen sync.RWMutex

	//data channel to accept the incoming data
	dataChan chan *base.WrappedMCRequest
	
	//the total size of data (in byte) queued in dataChan
	bytes_in_dataChan int32
	dataChan_control  chan bool
	
	//configurable parameter
	config messagingConfig
	
	
	mClient      *natsStreamingClient  //TODO : will need to abrstract this out to hide messaging server
	lock_mClient sync.RWMutex   //TODO : Do we need in this place ro should be hidden inside MessagHandler ?

	childrenWaitGrp sync.WaitGroup

	//buffer for the sent, but not yet confirmed data
	buf *requestBuffer

	
	sender_finch      chan bool
	receiver_finch    chan bool
	checker_finch     chan bool
	selfMonitor_finch chan bool

	counter_sent     uint32
	counter_received uint32
	counter_waittime uint32
	counter_batches  int32
	start_time       time.Time
}


func NewMessagingNozzle(id string,
	topic string,
	connectString string,
	username string,
	password string,
	certificate []byte,
	logger_context *log.LoggerContext) *MessagingNozzle {

	//callback functions from GenServer
	var msg_callback_func gen_server.Msg_Callback_Func
	var exit_callback_func gen_server.Exit_Callback_Func
	var error_handler_func gen_server.Error_Handler_Func

	server := gen_server.NewGenServer(&msg_callback_func,
		&exit_callback_func, &error_handler_func, logger_context, "MessagingNozzle")
	part := NewAbstractPartWithLogger(id, server.Logger())

	messaging := &MessagingNozzle{GenServer: server, /*gen_server.GenServer*/
		AbstractPart:        part,                           /*part.AbstractPart*/
		bOpen:               true,                           /*bOpen	bool*/
		lock_bOpen:          sync.RWMutex{},                 /*lock_bOpen	sync.RWMutex*/
		config:              newMessagingConfig(server.Logger()), /*config	capiConfig*/
		childrenWaitGrp:     sync.WaitGroup{},               /*childrenWaitGrp sync.WaitGroup*/
		sender_finch:        make(chan bool, 1),
		checker_finch:       make(chan bool, 1),
		selfMonitor_finch:   make(chan bool, 1),
		counter_sent:      0,
		counter_received:  0,
	}

	messaging.config.connectStr = connectString
	messaging.config.username = username
	messaging.config.password = password
	
	msg_callback_func = nil
	exit_callback_func = messaging.onExit
	error_handler_func = messaging.handleGeneralError

	return messaging

}

func (messaging *MessagingNozzle) IsOpen() bool {
	messaging.lock_bOpen.RLock()
	defer messaging.lock_bOpen.RUnlock()
	return messaging.bOpen
}

func (messaging *MessagingNozzle) Open() error {
	messaging.lock_bOpen.Lock()
	defer messaging.lock_bOpen.Unlock()
	if !messaging.bOpen {
		messaging.bOpen = true

	}
	return nil
}

func (messaging *MessagingNozzle) Close() error {
	messaging.lock_bOpen.Lock()
	defer messaging.lock_bOpen.Unlock()
	if messaging.bOpen {
		messaging.bOpen = false
	}
	return nil
}

func (messaging *MessagingNozzle) msgClient() *natsStreamingClient {
	messaging.lock_mClient.RLock()
	defer messaging.lock_mClient.RUnlock()
	return messaging.mClient
}

func (messaging *MessagingNozzle) Start(settings map[string]interface{}) error {
	messaging.Logger().Infof("%v starting ....\n", messaging.Id())

	err := messaging.SetState(common.Part_Starting)
	if err != nil {
		return err
	}

  // Intialize messaging nozzle 
	err = messaging.initialize(settings)
	
	messaging.Logger().Infof("%v initialized\n", messaging.Id())
	if err == nil {
		messaging.childrenWaitGrp.Add(1)
		go messaging.selfMonitor(messaging.selfMonitor_finch, &messaging.childrenWaitGrp)

		//messaging.childrenWaitGrp.Add(1)
		//go messaging.processData(messaging.sender_finch, &messaging.childrenWaitGrp)

		messaging.start_time = time.Now()
		err = messaging.Start_server()
	}

	if err == nil {
		err = messaging.SetState(common.Part_Running)
		if err == nil {
			messaging.Logger().Infof("%v has been started successfully\n", messaging.Id())
		}
	}
	if err != nil {
		messaging.Logger().Errorf("%v failed to start. err=%v\n", messaging.Id(), err)
	}
	return err
}



func (messaging *MessagingNozzle) initialize(settings map[string]interface{}) error {
	//err := messaging.config.initializeConfig(settings)
	//if err != nil {
	//	return err
	//}

	//TODO : All data channel and other intialization need to be done here
	messaging.Logger().Infof("%v messaging client about to be created", messaging.Id())
	messaging.checker_finch = make(chan bool, 1)
    messaging.mClient = newNatsStreamingClient("test-clutser","messaging-client", messaging.Logger())
    if messaging.mClient != nil {  
       messaging.Logger().Infof("%v messaging client successfully created", messaging.Id())
     }
    
	
	
	/*if err == nil {
		messaging.Logger().Infof("%v connection initialization completed.", messaging.Id())
	} else {
		messaging.Logger().Errorf("%v connection initialization failed with err=%v.", messaging.Id(), err)
	}*/

	return nil
}


func (messaging *MessagingNozzle) selfMonitor(finch chan bool, waitGrp *sync.WaitGroup) {
	defer waitGrp.Done()
	statsTicker := time.NewTicker(messaging.config.selfMonitorInterval)
	defer statsTicker.Stop()
	for {
		select {
		case <-finch:
			goto done
	  //TODO : need another ticker so that regular logging of data recived and sent to be logged		
		case <-statsTicker.C:
		    //TODO: need to publish statistics
			//messaging.RaiseEvent(common.NewEvent(common.StatsUpdate, nil, messaging, nil, []int{int(atomic.LoadInt32(20)), int(atomic.LoadInt32(20))}))
			messaging.Logger().Debugf("%v selfMonitor stats ticker", messaging.Id())
		}
	}
done:
	messaging.Logger().Infof("%v selfMonitor routine exits", messaging.Id())

}


func (messaging *MessagingNozzle) Stop() error {
	messaging.Logger().Infof("%v stopping \n", messaging.Id())

	err := messaging.SetState(common.Part_Stopping)
	if err != nil {
		return err
	}

	messaging.Logger().Debugf("%v processed %v items\n", messaging.Id(), atomic.LoadUint32(&messaging.counter_sent))

	//close data channels
	//for _, dataChan := range capi.vb_dataChan_map {
		//close(dataChan)
	//}

	//if capi.batches_ready != nil {
		//capi.Logger().Infof("%v closing batches ready\n", capi.Id())
		//close(capi.batches_ready)
	//}

	err = messaging.Stop_server()

	err = messaging.SetState(common.Part_Stopped)
	if err == nil {
		messaging.Logger().Infof("%v has been stopped\n", messaging.Id())
	} else {
		messaging.Logger().Errorf("%v failed to stop. err=%v\n", messaging.Id(), err)
	}

	return err
}

func (messaging *MessagingNozzle) onExit() {
	//in the process of stopping, no need to report any error to replication manager anymore
	//messaging.disableHandleError()

	//notify the data processing routine
	close(messaging.sender_finch)
	close(messaging.checker_finch)
	close(messaging.selfMonitor_finch)
	messaging.childrenWaitGrp.Wait()

	//cleanup
	messaging.Logger().Infof("%v releasing Messaging Handler", messaging.Id())
	mc := messaging.msgClient()
	if mc != nil {
		mc.close()
	}

}


func (messaging *MessagingNozzle) Receive(data interface{}) error {
	defer func() {
		if r := recover(); r != nil {
			messaging.Logger().Errorf("%v recovered from %v", messaging.Id(), r)
			messaging.handleGeneralError(errors.New(fmt.Sprintf("%v", r)))
		}
	}()

	err := messaging.validateRunningState()
	if err != nil {
		messaging.Logger().Infof("%v is in %v state, Recieve did no-op", messaging.Id(), messaging.State())
		return err
	}

	request, ok := data.(*base.WrappedMCRequest)

	if !ok {
		err = fmt.Errorf("Got data of unexpected type. data=%v", data)
		messaging.Logger().Errorf("%v %v", messaging.Id(), err)
		messaging.handleGeneralError(errors.New(fmt.Sprintf("%v", err)))
		return err

	}

	messaging.Logger().Infof("%v data key=%v seq=%v is received", messaging.Id(), request.Req.Key, data.(*base.WrappedMCRequest).Seqno)
	messaging.Logger().Infof("%v data channel len is %d\n", messaging.Id(), len(messaging.dataChan))

   
	//TODO: Need to move out this logic to seprate  go routine
	
	//Convert MCRequest object to Byte
	 bData :=  mcRequestToByte(request)
	 messaging.Logger().Infof("Coverted Bytes :  %v \n", bData)
	//for now we will directly call publish
	messaging.msgClient().publish("test-bucket", bData)
	

	messaging.Logger().Infof("%v received %v items, queue_size = %v\n", messaging.Id(), atomic.LoadUint32(&messaging.counter_received), len(messaging.dataChan))

	return nil
}


func (messaging *MessagingNozzle) validateRunningState() error {
	state := messaging.State()
	if state == common.Part_Stopping || state == common.Part_Stopped || state == common.Part_Error {
		return PartStoppedError
	}
	return nil
}

func (messaging *MessagingNozzle) handleGeneralError(err error) {
	if true {
		messaging.Logger().Errorf("%v raise error condition %v\n", messaging.Id(), err)
		messaging.RaiseEvent(common.NewEvent(common.ErrorEncountered, nil, messaging, nil, err))
	} else {
		messaging.Logger().Debugf("%v in shutdown process, err=%v is ignored\n", messaging.Id(), err)
	}
}





func mcRequestToByte(req *base.WrappedMCRequest) []byte  {
   doc_map := getDocumentMap(req.Req)
   doc_bytes, err := json.Marshal(doc_map)
		if err != nil {
			return doc_bytes
		}
		return doc_bytes
}



// TODO : move this to some utility file .... produce a serialized document from mc request
func getDocumentMap(req *mc.MCRequest) map[string]interface{} {
	doc_map := make(map[string]interface{})
	meta_map := make(map[string]interface{})
	doc_map[MetaKey] = meta_map
	doc_map[BodyKey] = req.Body

	//TODO need to handle Key being non-UTF8?
	meta_map[IdKey] = string(req.Key)
	meta_map[RevKey] = getSerializedRevision(req)
	meta_map[ExpirationKey] = binary.BigEndian.Uint32(req.Extras[4:8])
	meta_map[FlagsKey] = binary.BigEndian.Uint32(req.Extras[0:4])
	if req.Opcode == base.DELETE_WITH_META {
		meta_map[DeletedKey] = true
	}

	if !simple_utils.IsJSON(req.Body) {
		meta_map[AttReasonKey] = InvalidJson
	}

	return doc_map
}


////for now we will keep nats messaging here

/************************************
/* struct Config
*************************************/
type natsStreamingClient struct {
	clusterID 		   string
	clientID 	       string
	URL 			   string
	client     		   stan.Conn
	logger             *log.CommonLogger
}

func newNatsStreamingClient(cluID string,
	cliID string,
	logger_context *log.CommonLogger) *natsStreamingClient {
    
    var URL string
    
    flag.StringVar(&URL, "s", stan.DefaultNatsURL, "The nats server URLs (separated by comma)")
	flag.StringVar(&URL, "server", stan.DefaultNatsURL, "The nats server URLs (separated by comma)")
	flag.StringVar(&cluID, "c", "test-cluster", "The NATS Streaming cluster ID")
	flag.StringVar(&cluID, "cluster", "test-cluster", "The NATS Streaming cluster ID")
	flag.StringVar(&cliID, "id", "stan-pub", "The NATS Streaming client ID to connect with")
	flag.StringVar(&cliID, "clientid", "stan-pub", "The NATS Streaming client ID to connect with")
	flag.Parse()

	sc, err := stan.Connect(cluID, cliID, stan.NatsURL(URL))
    
	if err != nil {
		logger_context.Errorf("Can't connect: %v.\nMake sure a NATS Streaming Server is running at: %s", err,stan.DefaultNatsURL)
		return nil
	}
	
     return &natsStreamingClient{
     	clusterID: cluID,
     	clientID: cliID,
     	URL:stan.DefaultNatsURL,
     	client: sc,
     	logger:logger_context,
     }
}

func(nss *natsStreamingClient) close() {
	if nss.client != nil {
		nss.client.Close()
	}
}

func(nss *natsStreamingClient) publish(subj string, msg []byte) {
	 err := nss.client.Publish(subj, msg)
		if err != nil {
			nss.logger.Errorf("Error during publish: %v\n", err)
		}
		nss.logger.Debugf("Published [%s] : '%s'\n", subj, msg)
}







