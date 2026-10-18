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

const Version = "1.0.5"

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

	// Используем явный net.Listen("tcp4"), чтобы избежать конфликтов IPv4/IPv6 при разрешении localhost
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
		// Обрабатываем каждое клиентское соединение в изолированной горутине
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

	// Гарантированная и безопасная очистка ресурсов сессии при выходе
	defer func() {
		for _, aw := range mirrorWriters {
			aw.Close() // Сигнализирует воркеру о закрытии и дожидается его завершения
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
	allWriters = append(allWriters, primary) // Первым пишем в primary

	for _, m := range mirrors {
		aw := newAsyncWriter(m)
		mirrorWriters = append(mirrorWriters, aw)
		allWriters = append(allWriters, aw)
	}

	// Мультиплексируем поток данных: сначала идет запись в primary, затем в очереди зеркал
	mw := io.MultiWriter(allWriters...)

	var wg sync.WaitGroup
	wg.Add(2)

	// Горутина: Клиент -> (Primary + Зеркала)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(mw, in)
		if tcpConn, ok := primary.(*net.TCPConn); ok {
			_ = tcpConn.CloseWrite()
		}
	}()

	// Горутина: Primary -> Клиент
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

// asyncWriter изолирует медленные зеркала, предотвращая задержки на основном сервере
type asyncWriter struct {
	conn      net.Conn
	queue     chan []byte
	closed    chan struct{} // Сигнальный маркер для неблокирующей проверки статуса
	wg        sync.WaitGroup
	closeOnce sync.Once // Гарантирует, что сигнал закрытия выполнится строго 1 раз
}

func newAsyncWriter(conn net.Conn) *asyncWriter {
	aw := &asyncWriter{
		conn:   conn,
		queue:  make(chan []byte, 100), // Буфер на 100 пакетов
		closed: make(chan struct{}),
	}
	aw.wg.Add(1)
	go aw.worker()
	return aw
}

func (aw *asyncWriter) worker() {
	defer aw.wg.Done()

	for {
		select {
		case <-aw.closed:
			// Основной поток вызвал Close(), завершаем работу горутины
			return
		case buf, ok := <-aw.queue:
			if !ok {
				return
			}
			_, err := aw.conn.Write(buf)
			if err != nil {
				if *debugPtr {
					log.Printf("Mirror %s write error: %v. Stopping worker.", aw.conn.RemoteAddr(), err)
				}
				// При ошибке записи в зеркало переводим структуру в статус закрытой.
				// Канал aw.queue намеренно ОСТАЕТСЯ открытым, чтобы уберечь метод Write от паники.
				aw.stop()
				return
			}
		}
	}
}

func (aw *asyncWriter) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	// 1. Быстрая проверка: если писатель закрыт, мгновенно дропаем пакет
	select {
	case <-aw.closed:
		return len(p), nil
	default:
	}

	// Клонируем слайс байт до отправки в очередь во избежание Race Condition со стороны io.Copy
	buf := make([]byte, len(p))
	copy(buf, p)

	// 2. Защищенная каскадная проверка через select
	select {
	case <-aw.closed:
		return len(p), nil
	default:
		// Так как канал queue никогда не закрывается во время работы Write,
		// паника 'send on closed channel' физически невозможна из-за специфики выбора кейсов в select
		select {
		case <-aw.closed:
			return len(p), nil
		case aw.queue <- buf:
			// Пакет успешно добавлен в буфер отправки зеркала
		default:
			// Буфер переполнен или воркер остановлен. Дропаем пакет ради стабильности primary
			if *debugPtr {
				log.Printf("Mirror %s queue full or stopped, dropping packet", aw.conn.RemoteAddr())
			}
		}
	}

	// Всегда возвращаем успех для MultiWriter, чтобы проблемы зеркал не влияли на сессию с primary
	return len(p), nil
}

func (aw *asyncWriter) stop() {
	aw.closeOnce.Do(func() {
		close(aw.closed) // Закрываем только сигнальный канал
	})
}

func (aw *asyncWriter) Close() {
	aw.stop()
	aw.wg.Wait() // Ожидаем штатного выхода фонового воркера
}

func Usage() {
	fmt.Fprintf(os.Stderr, "tcpmirror version %s\n", Version)
	fmt.Fprintf(os.Stderr, "Usage: tcpmirror -l <listen_addr> -p <primary_addr> -m <mirror_addrs>\n")
	flag.PrintDefaults()
}
