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

const Version = "1.0.7"

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
			aw.Close() // Сигнализирует воркеру о закрытии, дописывает буфер и дожидается его завершения
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
		if buf == nil {
			continue // Пропускаем проверочные маркеры из Fast-Path
		}
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
			}
			aw.mu.Unlock()

			// Опустошаем остатки очереди в никуда, чтобы io.Copy не заблокировался,
			// если он проскочил проверку mu.RLock() в методе Write
			for range aw.queue {}
			return
		}
	}
}

func (aw *asyncWriter) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	// Читающая блокировка RLock быстра и эффективна. Она гарантирует,
	// что мы не начнем писать в канал, если он сейчас параллельно закрывается.
	aw.mu.RLock()
	defer aw.mu.RUnlock()

	if aw.isClosed {
		return len(p), nil // Молча дропаем данные, если зеркало закрыто или вышло из строя
	}

	// ОПТИМИЗАЦИЯ FAST-PATH: Проверяем, есть ли вообще место в очереди БЕЗ аллокации памяти.
	// Если канал полон, сразу уходим в default и дропаем пакет, не нагружая Garbage Collector.
	select {
	case aw.queue <- nil: // Пробуем закинуть пустой маркер для проверки емкости
		<-aw.queue // Место есть! Мгновенно забираем проверочный nil обратно
	default:
		if *debugPtr {
			log.Printf("Mirror %s queue full, dropping packet (fast-path)", aw.conn.RemoteAddr())
		}
		return len(p), nil
	}

	// Выделяем память только тогда, когда точно уверены, что пакет поместится
	buf := make([]byte, len(p))
	copy(buf, p)

	select {
	case aw.queue <- buf:
		// Пакет успешно добавлен в буфер отправки зеркала
	default:
		// Крайне редкий случай: если за наносекунду между проверкой и записью место заняла другая горутина
		if *debugPtr {
			log.Printf("Mirror %s queue full, dropping packet (race-path)", aw.conn.RemoteAddr())
		}
	}

	// Всегда возвращаем успех для MultiWriter, чтобы проблемы зеркал не влияли на сессию с primary
	return len(p), nil
}

func (aw *asyncWriter) Close() {
	aw.mu.Lock()
	if !aw.isClosed {
		aw.isClosed = true
		close(aw.queue) // Наш единственный триггер для запуска завершения range в воркере
	}
	aw.mu.Unlock()

	aw.wg.Wait() // Ожидаем, пока фоновый воркер допишет остатки данных из буфера и выйдет
}

func Usage() {
	fmt.Fprintf(os.Stderr, "tcpmirror version %s\n", Version)
	fmt.Fprintf(os.Stderr, "Usage: tcpmirror -l <listen_addr> -p <primary_addr> -m <mirror_addrs>\n")
	flag.PrintDefaults()
}
