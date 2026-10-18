package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

const Version = "1.0.2"

var (
	listenPtr  = flag.String("l", "localhost:8080", "Listen on `host:port` for incoming traffic")
	primaryPtr = flag.String("p", "localhost:9090", "Relay traffic to primary `host:port` (two-way)")
	mirrorPtr  = flag.String("m", "localhost:9091", "Mirror traffic to `host:port[,host:port]...` (one-way)")
	debugPtr   = flag.Bool("d", false, "Print debug information to stdout")
)

func main() {
	flag.Usage = Usage
	flag.Parse()

	mirrorAddrs := strings.Split(*mirrorPtr, ",")
	log.Printf("Starting tcpmirror v%s\n", Version)
	log.Printf("[*] Listening on %s\n", *listenPtr)
	log.Printf("[*] Primary backend: %s\n", *primaryPtr)
	log.Printf("[*] Mirror backends: %v\n", mirrorAddrs)

	l, err := net.Listen("tcp4", *listenPtr)
	if err != nil {
		log.Fatalf("Critical: Error listening on %s: %v", *listenPtr, err)
	}
	defer l.Close()

	for {
		in, err := l.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		go handleConnection(in, *primaryPtr, mirrorAddrs)
	}
}

func handleConnection(in net.Conn, primaryAddr string, mirrorAddrs []string) {
	defer in.Close()

	if *debugPtr {
		log.Printf("New connection from %s", in.RemoteAddr())
	}

	primary, err := net.DialTimeout("tcp4", primaryAddr, 5*time.Second)
	if err != nil {
		log.Printf("Error connecting to primary %s: %v", primaryAddr, err)
		return
	}
	defer primary.Close()

	var mirrors []net.Conn
	var mirrorWriters []*asyncWriter

	// Корректное закрытие всех ресурсов
	defer func() {
		for _, aw := range mirrorWriters {
			aw.Close() // Закрывает канал и ждет завершения воркера
		}
		for _, m := range mirrors {
			_ = m.Close()
		}
	}()

	for _, addr := range mirrorAddrs {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		m, err := net.DialTimeout("tcp4", addr, 3*time.Second)
		if err != nil {
			log.Printf("Warning: Skipping mirror %s (unreachable): %v", addr, err)
			continue
		}
		mirrors = append(mirrors, m)
	}

	var allWriters []io.Writer
	allWriters = append(allWriters, primary)

	for _, m := range mirrors {
		aw := newAsyncWriter(m)
		mirrorWriters = append(mirrorWriters, aw)
		allWriters = append(allWriters, aw)
	}

	mw := io.MultiWriter(allWriters...)

	var wg sync.WaitGroup
	wg.Add(2)

	// Клиент -> (Primary + Зеркала)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(mw, in)
		if tcpConn, ok := primary.(*net.TCPConn); ok {
			_ = tcpConn.CloseWrite()
		}
	}()

	// Primary -> Клиент
	go func() {
		defer wg.Done()
		_, _ = io.Copy(in, primary)
		if tcpConn, ok := in.(*net.TCPConn); ok {
			_ = tcpConn.CloseWrite()
		}
	}()

	wg.Wait()

	if *debugPtr {
		log.Printf("Connection from %s closed", in.RemoteAddr())
	}
}

type asyncWriter struct {
	conn net.Conn
	queue chan []byte
	wg    sync.WaitGroup
}

func newAsyncWriter(conn net.Conn) *asyncWriter {
	aw := &asyncWriter{
		conn:  conn,
		queue: make(chan []byte, 100),
	}
	aw.wg.Add(1)
	go aw.worker()
	return aw
}

func (aw *asyncWriter) worker() {
	defer aw.wg.Done()
	for buf := range aw.queue {
		_, err := aw.conn.Write(buf)
		if err != nil {
			if *debugPtr {
				log.Printf("Mirror %s write error: %v. Dropping remaining traffic.", aw.conn.RemoteAddr(), err)
			}
			// Опустошаем канал в случае ошибки сокета, чтобы предотвратить блокировку основного потока
			for range aw.queue {}
			return
		}
	}
}

func (aw *asyncWriter) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	// Выделяем память и копируем данные ДО отправки в канал.
	// Это гарантирует изолированность данных от io.Copy.
	buf := make([]byte, len(p))
	copy(buf, p)

	select {
	case aw.queue <- buf:
	default:
		if *debugPtr {
			log.Printf("Mirror %s queue full, dropping packet", aw.conn.RemoteAddr())
		}
	}

	return len(p), nil
}

func (aw *asyncWriter) Close() {
	close(aw.queue)
	aw.wg.Wait() // Ждем, пока воркер штатно завершит работу
}

func Usage() {
	fmt.Fprintf(os.Stderr, "tcpmirror version %s\n", Version)
	fmt.Fprintf(os.Stderr, "Usage: tcpmirror -l <listen_addr> -p <primary_addr> -m <mirror_addrs>\n")
	flag.PrintDefaults()
}
