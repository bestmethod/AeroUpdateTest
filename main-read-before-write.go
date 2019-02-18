package main

import (
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)
import "github.com/bestmethod/go-logger"
import as "github.com/aerospike/aerospike-client-go"

// so we can just do logger.this and logger.that
var logger Logger.Logger

// golang entrypoint
func main() {
	// we like structs and objects, lets have a struct-main :)
	m := mainStruct{}
	m.main()
}

// this is the real entry point
func (m *mainStruct) main() {
	m.setLogger()
	m.osArgs()
	m.mainLoop()
}

// random string generator
func RandStringRunes(n int) string {
	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

// the main loop which never ends - the actual monitoring loop
func (m *mainStruct) mainLoop() {

	// to randomize bin values
	rand.Seed(time.Now().UnixNano())

	// pk_max
	pk_max := 10000

	// lets do a limit of threads, 1k should be more than enough, it means 20 per second per downed server
	max_goroutines := 1000
	goroutines := make(chan int, max_goroutines)

	// some inter-goroutine-stats
	success := make(chan int, pk_max)

	// set some stuff
	pk := 0
	first_run := true

	// to infinity, and beyond
	for {

		// already has logic to handle existing connection and retries
		m.connect()

		// useful to know
		hang := false
		if len(goroutines) == max_goroutines {
			logger.Warn("Something is hanging, we have %d threads running. Going to wait for an available thread before continuing.", max_goroutines)
			hang = true
		}

		// this chan stuff is cool, basically, if we try to put more in than we can, it will just hang there and wait, auto-throttling
		goroutines <- 1

		// useful to know
		if hang == true {
			logger.Warn("Something was hanging, we had %d threads running. Continuing.", max_goroutines)
		}

		// we will be only inserting and/or updating PK=1 to 10000, over and over.
		pk = pk + 1
		if pk > pk_max {
			pk = 1
			for len(goroutines) > 1 {
				time.Sleep(time.Microsecond)
			}
			success_count := 0
			for len(success) > 0 {
				success_count += 1
				<-success
			}
			if first_run == true {
				logger.Info("InsertLoopComplete,%d,Threads=%d,SuccessCount=%d", pk_max, len(goroutines), success_count)
			} else {
				logger.Info("UpdateLoopComplete,%d,Threads=%d,SuccessCount=%d", pk_max, len(goroutines), success_count)
			}
			first_run = false
		}

		// wrap around in a goroutine to run async - in case one node needs a timeout as it hangs
		go func(pk int, goroutines chan int, first_run bool, success chan int) {

			// lets handle panics
			defer func() {
				if r := recover(); r != nil {
					logger.Critical("Recovered in f: %s", r)
				}
			}()

			// we want the routine to note it exited once it is done, so the main thread know - for throttling, you see
			defer func(goroutines chan int) {
				<-goroutines
			}(goroutines)

			// set key and bin
			key, err := as.NewKey(m.ns, m.set, pk)
			if err != nil {
				if m.errors == true {
					logger.Error("Could not create Key from PK %d: %s", pk, err)
				}
				return
			}
			bin := as.NewBin(m.binName, RandStringRunes(m.binLen))

			// read before write bit added now
			if first_run == false {
				a := as.NewPolicy()
				a.ReplicaPolicy = as.MASTER_PROLES
				a.ConsistencyLevel = as.CONSISTENCY_ONE
				a.Timeout = 50 * time.Millisecond
				a.SocketTimeout = a.Timeout
				rec, err := m.client.Get(a, key, m.binName)
				if err != nil {
					if m.errors == true {
						logger.Error("Could not get bins for PK %d: %s", pk, err)
					}
					return
				}
				if rec != nil {
					key = rec.Key
				} else {
					if m.errors == true {
						logger.Error("GET() returned NIL, record not found for PK %d",pk)
					}
				}
			}

			// lets set the policy
			policy := as.NewWritePolicy(0, 0)
			policy.Timeout = 50 * time.Millisecond
			policy.SocketTimeout = policy.Timeout

			// do the inserts only on first run, and updates only on all subsequent runs
			if first_run == true {
				policy.RecordExistsAction = as.CREATE_ONLY
			} else {
				policy.RecordExistsAction = as.UPDATE_ONLY
			}

			// work
			err = m.client.PutBins(policy, key, bin)
			if err != nil {
				if m.errors == true {
					logger.Error("Could not put bins for PK %d: %s", pk, err)
				}
				return
			}

			// make this as success
			success <- 1

			// just to see it works
			if m.debug == true {
				if first_run == true {
					logger.Debug("InsertSuccess,Key=%d,Threads=%d", pk, len(goroutines))
				} else {
					logger.Debug("UpdateSuccess,Key=%d,Threads=%d", pk, len(goroutines))
				}
			}

		}(pk, goroutines, first_run, success)

		// short snooze before the loop ;)
		time.Sleep(500 * time.Microsecond)
	}
}

// process the os.Args[] arguments into the struct with what the user filled in. Provide usage if needed.
func (m *mainStruct) osArgs() {
	var err error
	if len(os.Args) < 8 || len(os.Args) == 9 || len(os.Args) > 10 {
		logger.Fatalf(1, "Incorrect usage.\nUsage: %s NodeIP NodePORT NamespaceName setName binName binValueLength debugLevel(0=JustSummaries,1=LogErrors,2=debug) [username] [password]", os.Args[0])
	}

	m.nodeIp = os.Args[1]
	m.nodePort, err = strconv.Atoi(os.Args[2])
	if err != nil {
		logger.Fatalf(2, "Port number incorrect: %s", os.Args[2])
	}
	m.ns = os.Args[3]
	m.set = os.Args[4]
	m.binName = os.Args[5]
	m.binLen, err = strconv.Atoi(os.Args[6])
	if err != nil {
		logger.Fatalf(2, "binValueLength incorrect: %s", os.Args[6])
	}
	if os.Args[7] == "2" {
		m.debug = true
		m.errors = true
	} else if os.Args[7] == "1" {
		m.debug = false
		m.errors = true
	} else {
		m.debug = false
		m.errors = false
	}
	if len(os.Args) == 10 {
		m.user = os.Args[8]
		m.pass = os.Args[9]
		m.policy = as.NewClientPolicy()
		m.policy.User = m.user
		m.policy.Password = m.pass
		m.policy.Timeout = 10 * time.Second
	}
}

// set the logger and configure it
func (m *mainStruct) setLogger() {
	logger = Logger.Logger{}
	err := logger.Init("", "AeroUpdateOnly", Logger.LEVEL_INFO|Logger.LEVEL_DEBUG, Logger.LEVEL_WARN|Logger.LEVEL_ERROR|Logger.LEVEL_CRITICAL, Logger.LEVEL_NONE)
	logger.TimeFormat("Jan 02 15:04:05.000000-0700")
	if err != nil {
		log.Fatalf("Logger init failed: %s", err)
	}
}

// connect to aerospike, will do up to 5 attempts at 100ms interval and die if no success. only do it IF not connected
func (m *mainStruct) connect() {
	var err error
	if m.client == nil || m.client.IsConnected() == false {
		for range []int{1, 2, 3, 4, 5} {

			logger.Info("ConnectingToCluster")
			if m.policy == nil {
				m.client, err = as.NewClient(m.nodeIp, m.nodePort)
			} else {
				m.client, err = as.NewClientWithPolicy(m.policy, m.nodeIp, m.nodePort)
			}
			if err != nil {
				logger.Error("ClientConnectFailed: %s", err)
				time.Sleep(100 * time.Millisecond)
			} else {
				logger.Info("ClientConnected")
				return
			}
		}
		logger.Fatalf(3, "Client connect failed after 5 attempts: %s", err)
	}
}

// the jewel ;) Yes, the struct for real main()
type mainStruct struct {
	nodeIp   string
	nodePort int
	client   *as.Client
	ns       string
	user     string
	pass     string
	set      string
	binName  string
	policy   *as.ClientPolicy
	binLen   int
	debug    bool
	errors   bool
}
