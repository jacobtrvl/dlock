// Copyright: Jacob Philip 2017
package main

import (
	"fmt"
	"html"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

var wg = new(sync.WaitGroup)
var confFile string

// Server ...
type Server string

// Heartbeat ...
type Heartbeat struct {
	lastrcvd   time.Time
	lastserver Server
}

// Vote ...
type Vote struct {
	server Server
	time   time.Time
}

// Configuration ...
type Configuration struct {
	serverName  string
	noOfServers int
}
type voteMap map[Server]bool

var lastVote Vote
var hb Heartbeat //Global structure for tracking heartbeat

var acceptVotes map[string]voteMap
var rejectVotes map[string]voteMap
var accepted map[string]bool

var ipmap map[Server]string
var lockmap map[string]Server

//Workaround for demo.
func createIPmap() {
	ipmap = make(map[Server]string)
	ipmap["dlock1"] = "192.168.33.10"
	ipmap["dlock2"] = "192.168.33.11"
	ipmap["dlock3"] = "192.168.33.12"
	ipmap["dlock4"] = "192.168.33.13"

}

func sendVotingReqToAll(serverid string, lockname string) {
	for _, ipaddr := range ipmap {
		sendReqToOne(serverid, ipaddr, "votingRequest", lockname)
	}
}

func sendReleaseReqToAll(serverid string, lockname string) {
	for _, ipaddr := range ipmap {
		sendReqToOne(serverid, ipaddr, "lockReleased", lockname)
	}
}

func sendReqToOne(serverid string, ipaddr string, reqType string, lockname string) {
	client := &http.Client{}
	//TODO move port number from here
	req, _ := http.NewRequest("POST", "http://"+ipaddr+":8080", nil)
	req.Header.Set("DlockReqType", reqType)
	req.Header.Set("Serverid", serverid)
	req.Header.Set("Lockname", lockname)
	//	log.Printf("Request = %v", req)
	resp, err := client.Do(req)
	if err == nil {
		defer resp.Body.Close()
		io.Copy(ioutil.Discard, resp.Body)
	}
}

func sendReply(ipaddr string, replyType string, lockname string) {
	client := &http.Client{}
	//TODO move port number from here
	req, _ := http.NewRequest("POST", "http://"+ipaddr+":9000", nil)
	req.Header.Set("Dlockreplytype", replyType)
	req.Header.Set("Lockname", lockname)
	//log.Printf("Request = %v", req)
	resp, err := client.Do(req)
	if err == nil {
		defer resp.Body.Close()
		io.Copy(ioutil.Discard, resp.Body)
	}
}

func getVotes(lockname string) {
	serverid := getserverid()
	accepted[lockname] = false
	for server := range acceptVotes[lockname] {
		acceptVotes[lockname][server] = false
	}
	for server := range rejectVotes[lockname] {
		rejectVotes[lockname][server] = false
	}
	sendVotingReqToAll(serverid, lockname)
	waitForVotes(lockname)

}

func waitForVotes(lockname string) {
	for j := 0; j <= 4; j++ {
		acceptCount := 0
		rejectCount := 0
		for _, value := range acceptVotes[lockname] {
			if value {
				acceptCount++
			}
			if acceptCount > 2 {
				accepted[lockname] = true
				return
			}
		}
		for _, value := range rejectVotes[lockname] {
			if value {
				rejectCount++
			}
			if rejectCount > 1 {
				accepted[lockname] = false
				return
			}
		}
		time.Sleep(time.Duration(1) * time.Second)
	}
}

func setLog(logpath string) {
	file, err := os.Create(logpath)
	if err != nil {
		panic(err)
	}
	log.SetOutput(io.Writer(file))
}

func requestedServer(id string) Server {
	//log.Printf("Request from server id %v\n", id)
	return Server(id)
}

func getserverip(server Server) string {
	return "http://" + ipmap[server] + ":8080"
}

func replyServerVote(server Server, serverid string, lockname string, status string) {
	var req *http.Request
	respServerIP := getserverip(server)
	client := &http.Client{}
	req, err := http.NewRequest("POST", respServerIP, nil)
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Set("DlockReqType", status)
	req.Header.Set("Serverid", serverid)
	req.Header.Set("Lockname", lockname)
	client.Do(req)
	log.Printf("Voted for server %v", server)
}
func vote(reqServer Server, serverid string, lockname string) {
	//update database
	//reply vote
	lockmap[lockname] = reqServer
	replyServerVote(reqServer, serverid, lockname, "votingAccept")
}

func reject(reqServer Server, serverid string, lockname string) {
	replyServerVote(reqServer, serverid, lockname, "votingReject")
}
func getserverid() string {
	conf := readConfig()
	return conf.serverName
}

func handleVote(reqServer Server, lockname string) {
	log.Printf("Got voting request from %v for lock  %v", reqServer, lockname)
	serverid := getserverid()
	value, exist := lockmap[lockname]
	if exist && value != "" {
		reject(reqServer, serverid, lockname)
	} else {
		if !exist {
			acceptVotes[lockname] = make(map[Server]bool)
			rejectVotes[lockname] = make(map[Server]bool)
		}
		vote(reqServer, serverid, lockname)
	}
}

func handleKeepAlive(reqServer Server) {
	//Handle heartbeats from master
	hb.lastrcvd = time.Now()
	hb.lastserver = reqServer
	log.Printf("Recieved keep alive from server %v", reqServer)
}

func addAcceptVote(reqServer Server, lockname string) {
	log.Printf("Accepted by %v lock %v", reqServer, lockname)
	acceptVotes[lockname][reqServer] = true
}
func addRejectVote(reqServer Server, lockname string) {
	log.Printf("Rejected by %v lock %v", reqServer, lockname)
	rejectVotes[lockname][reqServer] = true
}

func handleLockRequest(lockname string) {
	log.Printf("got request from client for lock : %v\n", lockname)
	value, exist := lockmap[lockname]
	if !exist {
		acceptVotes[lockname] = make(map[Server]bool)
		rejectVotes[lockname] = make(map[Server]bool)
	} else {
		if value != "" {
			sendReply("127.0.0.1", "lockReject", lockname)
			return
		}
	}
	getVotes(lockname)
	//TODO hardcoded IP
	if accepted[lockname] {
		sendReply("127.0.0.1", "lockAccept", lockname)
	} else {
		sendReleaseReqToAll(getserverid(), lockname)
		sendReply("127.0.0.1", "lockReject", lockname)
	}
}

func handleLockRelease(lockname string) {
	log.Printf("Request for lock release: %v", lockname)
	lockmap[lockname] = ""
	sendReleaseReqToAll(getserverid(), lockname)
	sendReply("127.0.0.1", "lockRel", lockname)
}

func releaseLock(reqServer Server, lockname string) {
	value, _ := lockmap[lockname]
	if value == reqServer {
		log.Printf("Got request from %v to release lock %v ", reqServer, lockname)
		lockmap[lockname] = ""
	}
}

func mesgHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hello, %q", html.EscapeString(r.URL.Path))
	headerAll := r.Header
	//log.Printf("header %v", headerAll)
	reqType := headerAll["Dlockreqtype"][0]
	switch reqType {
	case "lockReq":
		lockname := headerAll["Lockname"][0]
		handleLockRequest(lockname)
	case "lockRelease":
		lockname := headerAll["Lockname"][0]
		handleLockRelease(lockname)
	case "votingRequest":
		reqServer := requestedServer(headerAll["Serverid"][0])
		lockname := headerAll["Lockname"][0]
		handleVote(reqServer, lockname)
	case "votingAccept":
		reqServer := requestedServer(headerAll["Serverid"][0])
		lockname := headerAll["Lockname"][0]
		addAcceptVote(reqServer, lockname)
	case "votingReject":
		reqServer := requestedServer(headerAll["Serverid"][0])
		lockname := headerAll["Lockname"][0]
		addRejectVote(reqServer, lockname)
	case "lockReleased":
		reqServer := requestedServer(headerAll["Serverid"][0])
		lockname := headerAll["Lockname"][0]
		releaseLock(reqServer, lockname)
	case "keepAlive":
		reqServer := requestedServer(headerAll["Serverid"][0])
		handleKeepAlive(reqServer)
	}
}

func mesgListner(port string) {
	http.HandleFunc("/", mesgHandler)
	log.Fatal(http.ListenAndServe(port, nil))
	wg.Done()

}

func readConfig() Configuration {
	//TODO: Read from configuration file
	var conf Configuration
	conf.noOfServers = 4
	conf.serverName, _ = os.Hostname()
	log.Println(conf)
	return conf
}
func main() {
	acceptVotes = make(map[string]voteMap)
	rejectVotes = make(map[string]voteMap)
	accepted = make(map[string]bool)
	lockmap = make(map[string]Server)
	createIPmap()
	wg.Add(1)
	setLog("dlock.log")
	config := readConfig()
	log.Printf("Server Name: %v No of servers: %v", config.serverName, config.noOfServers)
	go mesgListner(":8080")
	wg.Wait()

}
