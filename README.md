# TCP Mirror

`tcpmirror` duplicates TCP traffic received on a port to more than one destination ports.
One of the destination is the primary server (port) which responds
to the incoming TCP traffic. Other streams are duplicate streams and
any response from the respective servers is discarded.

## Why

This is helpful in the following scenarios:

- To test a Dev/QA/secondary server with the same requests, traffic
and load that the primary production server handles.

- To do performance testing of candidate servers against the existing
production server.

- To re-write the server in another language and make sure that the
new server responds the same as existing server for same requests.

## Usage
```
tcpmirror -l <listen_addr> -p <primary_addr> -m <mirror_addrs1[,mirror_addrs2]> [-d]
```
- **-l**	Listen on `host:port` for incoming traffic
- **-p**	Relay traffic to primary `host:port` (two-way)
- **-m**	Mirror traffic to `host:port[,host:port]...` (one-way)
- **-d**	Print debug information to stdout

## Install

### Download from the releases pages

Download pre-built binary from the release page.

### Use `go get`

If you have `golang` tools installed, you can download and build the source code
locally as follows:
```
go get github.com/PeTrProduct/tcpmirror
```
The `tcpmirror` binary is now available in your `$GOPATH/bin` directory

### Run from source

```
git clone https://github.com/PeTrProduct/tcpmirror.git
cd tcpmirror
go run tcpmirror.go -l localhost:8080 -p localhost:9090 -m localhost:9091
```

### Compile from source

```
git clone https://github.com/PeTrProduct/tcpmirror.git
cd tcpmirror; 
go build ./tcpmirror.go
tcpmirror -l localhost:8080 -p localhost:9090 -m localhost:9091
```

## Run as a systemd service

```
useradd tcpmirror
ln -s /opt/tcpmirror/tcpmirror.service /usr/lib/systemd/system/
systemctl daemon-reload
systemctl start tcpmirror.service
```