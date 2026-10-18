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

const Version = "1.0.1"

var (
	listenPtr  = flag.String("l", "localhost:8080", "Listen on host:port for incoming traffic")
	primaryPtr = flag.String("p", "localhost:9090", "Relay traffic to primary host:port (two-way)")
	mirrorPtr  = flag.String("m", "localhost:9091", "Mirror traffic to host:port[,host:port]... (one-way)")
	debugPtr   = flag.Bool("d", false, "Print debug information to stdout")
)

func main() {
	flag.Usage = Usage
	flag.Parse()

Копировать
mirrorAddrs := strings.Split(*mirrorPtr, ",")
log.Printf("Starting tcpmirror v%s\n", Version)
log.Printf("[*] Listening on %s\n", *listenPtr)
log.Printf("[*] Primary backend: %s\n", *primaryPtr)
log.Printf("[*] Mirror backends: %v\n", mirrorAddrs)

// Явно слушаем только IPv4, чтобы избежать конфликтов разрешения адресов
l, err := net.Listen("tcp4", *listenPtr)
if err != nil {
	log.Fatalf("Critical: Error listening on %s: %v", *listenPtr, err)
}
defer l.Close()

for {
	in, err := l.Accept()
	if err != nil {
		log.Printf("Error accepting connection: %v", err)
		continue // Не падаем при ошибке сетевого интерфейса, ждем следующий синк
	}

	// Обрабатываем каждое клиентское соединение в изолированной горутине
	go handleConnection(in, *primaryPtr, mirrorAddrs)
}


}

func handleConnection(in net.Conn, primaryAddr string, mirrorAddrs []string) {
	defer in.Close()

Копировать
if *debugPtr {
	log.Printf("New connection from %s", in.RemoteAddr())
}

// 1. Подключаемся к основному серверу (IPv4)
primary, err := net.DialTimeout("tcp4", primaryAddr, 5*time.Second)
if err != nil {
	log.Printf("Error connecting to primary %s: %v", primaryAddr, err)
	return
}
defer primary.Close()

// 2. Подключаемся к зеркалам (IPv4)
var mirrors []net.Conn
var mirrorWriters []*asyncWriter

// Гарантируем закрытие ВСЕХ открытых зеркал и их каналов при выходе из функции
defer func() {
	for _, aw := range mirrorWriters {
		close(aw.queue) // Завершаем фоновую горутину записи
	}
	for _, m := range mirrors {
		_ = m.Close() // Закрываем сетевой сокет зеркала
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

// 3. Создаем пул асинхронных писателей для зеркал
var allWriters []io.Writer
allWriters = append(allWriters, primary) // Первым пишем в primary

for _, m := range mirrors {
	aw := &asyncWriter{conn: m, queue: make(chan []byte, 100)}
	mirrorWriters = append(mirrorWriters, aw)
	allWriters = append(allWriters, aw)
}

// Объединяем primary и обертки зеркал в один поток записи
mw := io.MultiWriter(allWriters...)

// Ожидаем завершения копирования данных в обе стороны
var wg sync.WaitGroup
wg.Add(2)

// Горутина: Клиент -> (Primary + Зеркала)
go func() {
	defer wg.Done()
	_, _ = io.Copy(mw, in)
	// Если клиент закрыл отправку, завершаем запись для primary
	if tcpConn, ok := primary.(*net.TCPConn); ok {
		_ = tcpConn.CloseWrite()
	}
}()

// Горутина: Primary -> Клиент
go func() {
	defer wg.Done()
	_, _ = io.Copy(in, primary)
	// Если primary закончил отвечать, закрываем запись для клиента
	if tcpConn, ok := in.(*net.TCPConn); ok {
		_ = tcpConn.CloseWrite()
	}
}()

wg.Wait()
if *debugPtr {
	log.Printf("Connection from %s closed", in.RemoteAddr())
}


}

// asyncWriter изолирует медленные зеркала, предотвращая лаги на основном сервере
type asyncWriter struct {
	conn net.Conn
	queue chan []byte
	once  sync.Once
}

func (aw *asyncWriter) Write(p []byte) (n int, err error) {
	// Ленивая инициализация фонового воркера при первой записи данных
	aw.once.Do(func() {
		go func() {
			for buf := range aw.queue {
				_, err := aw.conn.Write(buf)
				if err != nil {
					log.Printf("Mirror %s dropped connection: %v", aw.conn.RemoteAddr(), err)
					return // Выходим, горутина не потечет
				}
			}
		}()
	})

Копировать
// Клонируем слайс байт, чтобы избежать Race Condition при асинхронной обработке
buf := make([]byte, len(p))
copy(buf, p)

select {
case aw.queue <- buf:
	// Данные успешно добавлены в буфер отправки зеркала
default:
	// Буфер переполнен (зеркало не успевает читать). Дропаем пакет ради стабильности primary
	if *debugPtr {
		log.Printf("Mirror %s queue full, dropping packet to prevent lag", aw.conn.RemoteAddr())
	}
}

// Всегда возвращаем успех для MultiWriter, чтобы сбой зеркала не прерывал сессию primary
return len(p), nil 


}

func Usage() {
	fmt.Fprintf(os.Stderr, "tcpmirror version %s\n", Version)
	fmt.Fprintf(os.Stderr, "Usage: tcpmirror -l <listen_addr> -p <primary_addr> -m <mirror_addrs>\n")
	flag.PrintDefaults()
}