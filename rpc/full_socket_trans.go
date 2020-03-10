package rpc

import (
	"net"
	"time"
	"encoding/binary"
	"bufio"
	"errors"
	"sync"
	"math/rand"
	"context"
	"fmt"
)

const (
	RECONN_MIN = 500*time.Millisecond
	RECONN_MAX = 2*time.Second
	CONN_NUM = 2
	EXPIRE_TIME = 5*time.Second
)

type TransportLine struct {
	nextid uint32
	createConn  func() (net.Conn, error)
	conn net.Conn
	responses map[uint32] chan []byte
	sendCh chan *packet
	ctx context.Context
	cancel context.CancelFunc
	sync.RWMutex
	wg *sync.WaitGroup
	running bool
	reconnInterval time.Duration
	closeTime time.Duration
}


func (tsl *TransportLine) Run() {
	select {
		//已经收到退出信号
		case <- tsl.ctx.Done():
			tsl.wg.Done()
			return
		default:
	}
	conn, err := tsl.createConn()
	if err != nil {
		fmt.Printf("connect to hprose server error:%s\n", err.Error())
		//每次重连时间扩大2倍，直到2s
		time.Sleep(tsl.reconnInterval)
		if tsl.reconnInterval < RECONN_MAX {
			tsl.reconnInterval *= 2
		}
		go tsl.Run()
		return
	}
	ctime := time.Now().UnixNano()
	//如果重新连上的时间超过EXPIRE_TIME的话，排空之前所有请求
	if time.Duration(ctime) - tsl.closeTime > EXPIRE_TIME {
		tsl.drainRequest()
	}
	tsl.reconnInterval = RECONN_MIN
	tsl.conn = conn
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go tsl.sendLoop(wg)
	go tsl.readLoop(wg)
	wg.Wait()
	tsl.closeTime = time.Duration(time.Now().UnixNano())
	go tsl.Run()
}

func (tsl *TransportLine) readLoop(wg *sync.WaitGroup) {
	defer wg.Done()
	reader := bufio.NewReader(tsl.conn)
	var pkt packet
	loop:
	for {
		select {
		case <-tsl.ctx.Done():
			break loop
		default:
			if err := recvData(reader, &pkt); err != nil {
				break loop
			}
			pktId := binary.LittleEndian.Uint32(pkt.id[:])
			tsl.RLock()
			ch, ok := tsl.responses[pktId]
			tsl.RUnlock()
			if ok {
				data := make([]byte, len(pkt.body))
				copy(data, pkt.body)
				select {
				case ch <- data:
				default:
				}
			}
		}
	}
	tsl.conn.Close()
}

func (tsl *TransportLine) sendLoop(wg *sync.WaitGroup) {
	defer wg.Done()
	loop:
	for {
		select {
		case pkt := <- tsl.sendCh:
			err := sendData(tsl.conn, *pkt)
			if err != nil {
				break loop
			}
		case <-tsl.ctx.Done():
				break loop
		}
	}
	tsl.conn.Close()
}

func (tsl *TransportLine) sendAndReceive(
	data []byte, context *ClientContext) ([]byte, error) {
	//err := fd.conn.SetDeadline(time.Now().Add(context.Timeout))
	var err error
	var ret []byte
	retCh := make(chan []byte,1)
	tsl.Lock()
	if !tsl.running {
		tsl.running = true
		tsl.closeTime = time.Duration(time.Now().UnixNano())
		tsl.wg.Add(1)
		go tsl.Run()
	}
	tsl.nextid++
	pktId := tsl.nextid
	tsl.responses[pktId] = retCh
	tsl.Unlock()
	pkt := newPacket(true, pktId, data)
	if err == nil {
		err = tsl.send(pkt)
	}
	if err == nil {
		ret, err = tsl.recv(retCh, context.Timeout)
	}
	tsl.Lock()
	delete(tsl.responses, pktId)
	tsl.Unlock()
	return ret, err
}

func (tsl *TransportLine) send(pkt *packet) error {
	select {
	case tsl.sendCh <- pkt:
	default:
		return errors.New("sendCh is full")
	}
	return nil
}

func (tsl *TransportLine) recv(resCh chan []byte, timeout time.Duration) ([]byte,error) {
	select {
	case data := <- resCh:
		return data, nil
	case <- time.After(timeout)	:
		return nil, errors.New("recv timeout")
	}
}

func (tsl *TransportLine) drainRequest() {
	lp:
	for {
		select {
		case <-tsl.sendCh:
		default:
			break lp
		}
	}
}


type fullDuplexSocketTransport struct {
	createConn  func() (net.Conn, error)
	lineNum int
	lines []*TransportLine
	prtCtx context.Context
	prtCancel context.CancelFunc
	lineWg sync.WaitGroup
}

func newFullDuplexSocketTransport() (fd *fullDuplexSocketTransport) {
	fd = new(fullDuplexSocketTransport)
	fd.prtCtx, fd.prtCancel = context.WithCancel(context.Background())
	fd.lineNum = CONN_NUM
	fd.lines = make([]*TransportLine, fd.lineNum)
	for i := 0; i < fd.lineNum; i++ {
		fd.lines[i] = fd.NewTransportLine(fd.prtCtx, &fd.lineWg)
	}
	return
}

func (fd *fullDuplexSocketTransport) NewTransportLine(prt context.Context, wg *sync.WaitGroup) *TransportLine {
	tsl := &TransportLine{}
	tsl.wg = wg
	tsl.ctx, tsl.cancel = context.WithCancel(prt)
	tsl.createConn = fd.createConn
	tsl.nextid = 0
	tsl.responses = make(map[uint32]chan []byte)
	tsl.sendCh = make(chan *packet, 256*1024)
	tsl.running = false
	tsl.reconnInterval = RECONN_MIN
	return tsl
}

func (fd *fullDuplexSocketTransport) sendAndReceive(
	data []byte, context *ClientContext) ([]byte, error) {
	i := rand.Intn(fd.lineNum)
	return fd.lines[i].sendAndReceive(data, context)
}

func (fd *fullDuplexSocketTransport) setCreateConn(createConn func() (net.Conn, error)) {
	fd.createConn = createConn
	for _,ln := range fd.lines {
		ln.createConn = createConn
	}
}

func (fd *fullDuplexSocketTransport) close() {
	fd.prtCancel()
	fd.lineWg.Wait()
}




func (fd *fullDuplexSocketTransport)IdleTimeout() time.Duration {
	return 0
}
func (fd *fullDuplexSocketTransport)SetIdleTimeout(timeout time.Duration) {
}

func (fd *fullDuplexSocketTransport)MaxPoolSize() int {
	return 1
}
func (fd *fullDuplexSocketTransport)SetMaxPoolSize(size int) {

}

