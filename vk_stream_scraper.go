package main

import (
	"flag"
	"fmt"
	"github.com/gorilla/websocket"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"os/signal"
	"time"
)

type consoleParams struct {
	host string
	key  string
	help bool
}

var (
	argv consoleParams
)

func processArgs() (needStop bool) {
	needStop = true

	if argv.help {
		flag.Usage()
	} else if len(argv.host) == 0 {
		fmt.Fprintln(os.Stderr, "-host is required")
		flag.Usage()
	} else if len(argv.key) == 0 {
		fmt.Fprintln(os.Stderr, "-key is required")
		flag.Usage()
	} else {
		needStop = false
	}

	return
}

func init() {
	flag.StringVar(&argv.host, `host`, ``, `streaming api host. REQUIRED`)
	flag.StringVar(&argv.key, `key`, ``, `client key. REQUIRED`)
	flag.BoolVar(&argv.help, `h`, false, `show this help`)

	flag.Parse()
}

func connect(streamURL url.URL) (connection *websocket.Conn) {
	log.Printf("connecting to %s\n", streamURL.String())
	connection, response, err := websocket.DefaultDialer.Dial(streamURL.String(), nil)
	if err != nil {
		connection = nil
		if err == websocket.ErrBadHandshake {
			log.Printf("handshake failed with status %d\n", response.StatusCode)
			bodyBuf, _ := ioutil.ReadAll(response.Body)
			log.Println("respBody:", string(bodyBuf))
		}
		log.Fatal("dial error:", err)
	} else {
		log.Println("connection established")
	}
	return
}

func work(connection *websocket.Conn, done *chan struct{}) {
	for {
		_, message, err := connection.ReadMessage()
		if err != nil {
			log.Println("read error:", err)
			*done <- struct{}{}
			return
		}
		log.Printf("recv: %s", string(message))
	}
}

func waitEnd(connection *websocket.Conn, done *chan struct{}) {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	select {
	case <-interrupt:
		log.Println("interrupt")
		err := connection.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		if err != nil {
			log.Println("write close error: ", err)
			return
		}
		select {
		case <-*done:
		case <-time.After(time.Second):
		}
	case <-*done:
	}
}

func main() {
	if processArgs() {
		return
	}
	streamURL := url.URL{Scheme: "wss", Host: argv.host, Path: "/stream/", RawQuery: "key=" + argv.key}
	connection := connect(streamURL)
	if connection == nil {
		return
	}
	defer connection.Close()
	done := make(chan struct{})
	defer close(done)
	go work(connection, &done)
	waitEnd(connection, &done)
}