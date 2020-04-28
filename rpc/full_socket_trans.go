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
	//网络断开时，初始重连间隔
	RECONN_MIN = 500*time.Millisecond
	//网络断开时，最大重连间隔
	RECONN_MAX = 2*time.Second
	//保持的tcp连接数量
	CONN_NUM = 2
	//数据包过期时间
	EXPIRE_TIME = 5*time.Second
	//发送请求最大等待数量
    SendQueueSize = 256*1024
)

//rpc传输通道
type TransportLine struct {
	createConn  func() (net.Conn, error)
	//tcp连接
	conn net.Conn
	//待发送的rpc请求
	sendCh chan *packet
	//上级context
	prtCtx context.Context
	//本传输通道的contex
	ctx context.Context
	//本传输通道的取消函数
	cancel context.CancelFunc
	//上级用来管理传输通道
	wg *sync.WaitGroup
	//tcp重连间隔
	reconnInterval time.Duration

	//保护以下
	sync.RWMutex
	//下个rpc请求的序列号
	nextid uint32
	//每个请求对应的返回通道，以序列号为key
	responses map[uint32] chan []byte
	//判断本通道是否运行中
	running bool
	//本通道上次关闭时间
	closeTime time.Duration
}


func (tsl *TransportLine) Run() {
	select {
		//已经收到上级退出信号
		case <- tsl.prtCtx.Done():
			tsl.wg.Done()
			return
		default:
	}
	//重新运行时，创建新的context管理读写go程的生命期
	tsl.ctx, tsl.cancel = context.WithCancel(tsl.prtCtx)
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
	//网络连接成功后，重置下次重连时间间隔
	tsl.reconnInterval = RECONN_MIN
	tsl.conn = conn
	//同步读写go程的退出
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go tsl.sendLoop(wg)
	go tsl.readLoop(wg)
	wg.Wait()
	tsl.closeTime = time.Duration(time.Now().UnixNano())
	go tsl.Run()
}

//负责网络数据的读取，并将远程返回值压入对应的接收channel
func (tsl *TransportLine) readLoop(wg *sync.WaitGroup) {
	defer wg.Done()
	reader := bufio.NewReader(tsl.conn)
	var pkt packet
	loop:
	for {
		select {
		//该传输通道已结束
		case <-tsl.ctx.Done():
			break loop
		default:
			if err := recvData(reader, &pkt); err != nil {
				tsl.cancel()
				break loop
			}
			//获得序列号
			pktId := binary.LittleEndian.Uint32(pkt.id[:])
			tsl.RLock()
			ch, ok := tsl.responses[pktId]
			tsl.RUnlock()
			if ok {
				//将远端返回信息传入接收chan
				data := make([]byte, len(pkt.body))
				copy(data, pkt.body)
				select {
				case ch <- data:
				default:
				}
			}
		}
	}
	//fmt.Println("read end:", time.Now().Unix)
	tsl.conn.Close()
}

//负责将请求发送到对端
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
	//fmt.Println("send end:", time.Now().Unix)
	tsl.conn.Close()
}


//发送一个请求并等待返回结果
func (tsl *TransportLine) sendAndReceive(
	data []byte, context *ClientContext) ([]byte, error) {
	//err := fd.conn.SetDeadline(time.Now().Add(context.Timeout))
	var err error
	var ret []byte
	//用来接收返回值
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

//抛弃未发送的请求
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

//全双工传输
type fullDuplexSocketTransport struct {
	//创建网络连接的函数
	createConn  func() (net.Conn, error)
	//传输通道数量
	lineNum int
	//传输通道
	lines []*TransportLine
	//生命周期管理
	prtCtx context.Context
	prtCancel context.CancelFunc
	//用来同步各通道的结束
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
	tsl.prtCtx = prt
	tsl.createConn = fd.createConn
	tsl.nextid = 0
	tsl.responses = make(map[uint32]chan []byte)
	tsl.sendCh = make(chan *packet, SendQueueSize)
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

