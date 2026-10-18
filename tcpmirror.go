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

const Version = "1.1.1"

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
		// Сначала закрываем всех писателей (они внутри себя закроют связанные сокеты)
		for _, aw := range mirrorWriters {
			aw.Close() 
		}
		// Закрываем только те сокеты, для которых писатели НЕ успели создаться из-за сбоя
		for i := len(mirrorWriters); i < len(mirrors); i++ {
			_ = mirrors[i].Close()
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

	// Горутина: Primary -> Client
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
	conn     net.Conn
	queue    chan []byte
	mu       sync.RWMutex // Защищает флаг закрытия от состояния гонки с методом Write
	isClosed bool
	wg       sync.WaitGroup
}

func newAsyncWriter(conn net.Conn) *asyncWriter {
	aw := &asyncWriter{
		conn:  conn,
		queue: make(chan []byte, 100), // Буфер на 100 пакетов
	}
	aw.wg.Add(1)
	go aw.worker()
	return aw
}

func (aw *asyncWriter) worker() {
	defer aw.wg.Done()

	// Цикл range автоматически вычитает ВСЕ оставшиеся пакеты из буфера
	// после того, как канал будет закрыт (close(queue)), гарантируя отсутствие потери данных.
	for buf := range aw.queue {
		// Устанавливаем таймаут на запись, чтобы воркер не завис вечно при сетевых задержках
		_ = aw.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

		_, err := aw.conn.Write(buf)
		if err != nil {
			if *debugPtr {
				log.Printf("Mirror %s write error: %v. Stopping worker.", aw.conn.RemoteAddr(), err)
			}

			// Если сокет зеркала отвалился, атомарно закрываем прием новых данных
			aw.mu.Lock()
			if !aw.isClosed {
				aw.isClosed = true
				close(aw.queue)
				_ = aw.conn.Close() // Закрываем сокет, чтобы прервать заблокированные вызовы
			}
			aw.mu.Unlock()

			// Опустошаем остатки очереди в никуда, чтобы io.Copy не заблокировался
			for range aw.queue {}
			return
		}
	}
	
	// Сбрасываем дедлайн при успешном выходе из цикла
	_ = aw.conn.SetWriteDeadline(time.Time{})
}

func (aw *asyncWriter) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	// Читающая блокировка RLock быстра и эффективна.
	aw.mu.RLock()
	defer aw.mu.RUnlock()

	if aw.isClosed {
		return len(p), nil // Молча дропаем данные, если зеркало закрыто или вышло из строя
	}

	// ПОТОКОБЕЗОПАСНЫЙ FAST-PATH: Проверяем заполненность канала на лету.
	if len(aw.queue) >= cap(aw.queue) {
		if *debugPtr {
			log.Printf("Mirror %s queue full, dropping packet (fast-path)", aw.conn.RemoteAddr())
		}
		return len(p), nil
	}

	// Выделяем память только тогда, когда уверены, что в очереди было место
	buf := make([]byte, len(p))
	copy(buf, p)

	select {
	case aw.queue <- buf:
		// Пакет успешно добавлен в буфер отправки зеркала
	default:
		if *debugPtr {
			log.Printf("Mirror %s queue full, dropping packet (race-path)", aw.conn.RemoteAddr())
		}
	}

	return len(p), nil
}

func (aw *asyncWriter) Close() {
	aw.mu.Lock()
	if !aw.isClosed {
		aw.isClosed = true
		close(aw.queue)     // Сигнализируем range в воркере о завершении
		_ = aw.conn.Close() // Принудительно закрываем сетевой сокет
	}
	aw.mu.Unlock()

	aw.wg.Wait() // Ожидаем штатного выхода фонового воркера
}

func Usage() {
	fmt.Fprintf(os.Stderr, "tcpmirror version %s\n", Version)
	fmt.Fprintf(os.Stderr, "Usage: tcpmirror -l <listen_addr> -p <primary_addr> -m <mirror_addrs>\n")
	flag.PrintDefaults()
}
