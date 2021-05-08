package network

import (
	"bufio"
	"errors"
	"net"
	"strconv"
)

type Connnection struct {
	conn   *net.TCPConn
	reader Buffer
	writer *bufio.Writer
}

func NewConnection(conn *net.TCPConn) Connnection {
	return Connnection{
		conn:   conn,
		reader: newBuffer(conn),
		writer: bufio.NewWriter(conn),
	}
}

func (c *Connnection) RemoteAddr() string {
	return c.conn.RemoteAddr().String()
}

// ReadFrame 读取一个Frame
func (c *Connnection) ReadFrame() (*Frame, error) {
	for {
		// 1.当缓存区有足够正常的数据，则解析一个Frame返回
		frame, ok, err := c.parseFrame()
		if err != nil {
			return nil, err
		}
		if ok {
			return frame, nil
		}
		// 2.读满缓存区
		err = c.reader.readFromReader()
		if err != nil {
			return nil, err
		}
	}
}

func (c *Connnection) parseFrame() (*Frame, bool, error) {
	// 1.检查缓冲区数据能够读取一个Frame
	cursor := newCursor(c.reader.chunk())
	err := check(&cursor)
	if err != nil {
		if err == Incomplete {
			// 缓冲区数据不够
			return nil, false, nil
		} else {
			// 编码的Frame无效，终止连接
			return nil, false, errors.New("frame is invalid")
		}
	}
	// 2.读取一个Frame
	length := cursor.position()
	cursor.setPosition(0)
	frame, err := parse(&cursor)
	if err != nil {
		// 编码的Frame无效，终止连接
		return nil, false, errors.New("frame is invalid")
	}
	// 3.移动缓冲区的读取位置
	_ = c.reader.advance(length)
	return frame, true, nil
}

// WriteFrame 写入一个Frame
func (c *Connnection) WriteFrame(frame *Frame) error {
	switch frame.Ftype {
	case Array:
		// Encode the frame type prefix. For an array, it is '*'.
		if err := c.writer.WriteByte('*'); err != nil {
			return err
		}
		value, ok := frame.Value.([]*Frame)
		if !ok {
			return errors.New("unknown value")
		}
		// Encode the length of the array
		length := strconv.FormatInt(int64(len(value)), 10)
		if _, err := c.writer.WriteString(length + "\r\n"); err != nil {
			return err
		}
		// Iterate and encode each entry in the array.
		for _, v := range value {
			if err := c.writeValue(v); err != nil {
				return err
			}
		}
	default:
		if err := c.writeValue(frame); err != nil {
			return err
		}
	}
	return c.writer.Flush()
}

func (c *Connnection) writeValue(frame *Frame) error {
	switch frame.Ftype {
	case Simple:
		value, ok := frame.Value.(string)
		if !ok {
			return errors.New("unknown value")
		}
		if _, err := c.writer.WriteString("+" + value + "\r\n"); err != nil {
			return err
		}
	case Error:
		value, ok := frame.Value.(string)
		if !ok {
			return errors.New("unknown value")
		}
		if _, err := c.writer.WriteString("-" + value + "\r\n"); err != nil {
			return err
		}
	case Integer:
		value, ok := frame.Value.(int)
		if !ok {
			return errors.New("unknown value")
		}
		valstr := strconv.FormatInt(int64(value), 10)
		if _, err := c.writer.WriteString(":" + valstr + "\r\n"); err != nil {
			return err
		}
	case Null:
		if _, err := c.writer.WriteString("$-1\r\n"); err != nil {
			return err
		}
	case Bulk:
		value, ok := frame.Value.(string)
		if !ok {
			return errors.New("unknown value")
		}
		blen := strconv.FormatInt(int64(len(value)), 10)
		if _, err := c.writer.WriteString("$" + blen + "\r\n" + value + "\r\n"); err != nil {
			return err
		}
	case Array:
		value, ok := frame.Value.([]*Frame)
		if !ok {
			return errors.New("unknown value")
		}
		alen := strconv.FormatInt(int64(len(value)), 10)
		_, err := c.writer.WriteString("*" + alen + "\r\n")
		if err != nil {
			return err
		}
		for _, frame := range value {
			val, ok := frame.Value.(string)
			if !ok {
				return errors.New("unknown value")
			}
			alen := strconv.FormatInt(int64(len(val)), 10)
			_, err = c.writer.WriteString("$" + alen + "\r\n" + val + "\r\n")
			if err != nil {
				return err
			}
		}
	}
	return nil
}
