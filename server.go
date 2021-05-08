package gokvs

import (
	"fmt"
	"gokvs/cmd"
	"gokvs/engines"
	"gokvs/network"
	"net"
)

type KvsServer struct {
	db engines.KvsEngine
}

func NewKvsServer(engine engines.KvsEngine) *KvsServer {
	return &KvsServer{
		engine,
	}
}

func (s *KvsServer) Run(addr string) error {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return err
	}
	l, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return err
	}
	for {
		conn, err := l.AcceptTCP()
		if err != nil {
			return err
		}
		handler := Handler{
			db:         s.db,
			connection: network.NewConnection(conn),
		}
		go handler.run()
	}
}

type Handler struct {
	db         engines.KvsEngine
	connection network.Connnection
}

func (h *Handler) run() {
	for {
		// 1.读取一个Frame
		frame, err := h.connection.ReadFrame()
		if err != nil {
			// 网络读取Frame失败或无效协议无法解析，终止连接
			fmt.Printf("connection terminate %s, read frame error: %v\n", h.connection.RemoteAddr(), err)
			return
		}
		// 2.转换一个 Frame 为 command 结构
		command, err := cmd.FromFrame(frame)
		if err != nil {
			// 解析Frame是不支持的命令，终止连接
			fmt.Printf("connection terminate %s, convert command error: %v\n", h.connection.RemoteAddr(), err)
			return
		}
		// 3.执行命令进行db操作
		err = command.Apply(h.db, h.connection)
		if err != nil {
			fmt.Printf("connection terminate %s, command apply error: %v\n", h.connection.RemoteAddr(), err)
			return
		}
	}
}
