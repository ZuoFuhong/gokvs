package network

import (
	"errors"
	"strconv"
)

type FrameType int

const (
	Simple FrameType = iota
	Error
	Integer
	Bulk
	Null
	Array
)

var Incomplete = errors.New("not enough data is avalable to parse a message")

type Frame struct {
	Ftype FrameType
	Value interface{}
}

// 检查有足够正常的数据解析一个Frame
func check(c *Cursor) error {
	tb, err := getByte(c)
	if err != nil {
		return err
	}
	switch tb {
	case '+':
		_, err = getLine(c)
		if err != nil {
			return err
		}
	case '-':
		_, err = getLine(c)
		if err != nil {
			return err
		}
	case ':':
		_, err = getDecimal(c)
		if err != nil {
			return err
		}
	case '$':
		b, err := peekByte(c)
		if err != nil {
			return err
		}
		if b == '-' {
			// skip '-1\r\n'
			err := skip(c, 4)
			if err != nil {
				return err
			}
		} else {
			// Read the bulk string
			num, err := getDecimal(c)
			if err != nil {
				return err
			}
			// skip that number of bytes + 2 (\r\n)
			err = skip(c, num+2)
			if err != nil {
				return err
			}
		}
	case '*':
		num, err := getDecimal(c)
		if err != nil {
			return err
		}
		for i := 0; i < num; i++ {
			err = check(c)
			if err != nil {
				return err
			}
		}
	default:
		return errors.New("protocol error; invalid frame type byte " + string(tb))
	}
	return nil
}

// 解析一个Frame
func parse(c *Cursor) (*Frame, error) {
	tb, err := getByte(c)
	if err != nil {
		return nil, err
	}
	switch tb {
	case '+':
		line, err := getLine(c)
		if err != nil {
			return nil, err
		}
		frame := &Frame{
			Ftype: Simple,
			Value: string(line),
		}
		return frame, nil
	case '-':
		line, err := getLine(c)
		if err != nil {
			return nil, err
		}
		frame := &Frame{
			Ftype: Error,
			Value: string(line),
		}
		return frame, nil
	case ':':
		length, err := getDecimal(c)
		if err != nil {
			return nil, err
		}
		frame := &Frame{
			Ftype: Integer,
			Value: length,
		}
		return frame, nil
	case '$':
		tb, err := peekByte(c)
		if err != nil {
			return nil, err
		}
		if string(tb) == "-" {
			if string(tb) == "-1" {
				return nil, errors.New("protocol error; invalid frame format")

			}
			frame := &Frame{
				Ftype: Null,
				Value: nil,
			}
			return frame, nil
		} else {
			// Read the bulk string
			length, err := getDecimal(c)
			if err != nil {
				return nil, err
			}
			n := length + 2
			if c.remaining() < n {
				return nil, Incomplete
			}
			data := make([]byte, length)
			copy(data, c.chunk()[:length])

			// skip that number of bytes + 2 (\r\n)
			err = skip(c, n)
			if err != nil {
				return nil, err
			}
			frame := &Frame{
				Ftype: Bulk,
				Value: string(data),
			}
			return frame, nil
		}
	case '*':
		length, err := getDecimal(c)
		if err != nil {
			return nil, err
		}
		tmpArr := make([]*Frame, 0)
		for i := 0; i < length; i++ {
			frame, err := parse(c)
			if err != nil {
				return nil, err
			}
			tmpArr = append(tmpArr, frame)
		}
		frame := &Frame{
			Ftype: Array,
			Value: tmpArr,
		}
		return frame, nil
	default:
		return nil, errors.New("not implemented")
	}
}

// 读取一行且不包含 '\r\n'
func getLine(c *Cursor) ([]byte, error) {
	start := c.pos
	end := len(c.buf)
	for i := start; i < end; i++ {
		if c.buf[i] == '\r' && c.buf[i+1] == '\n' {
			c.setPosition(i + 2)
			// return the line
			return c.buf[start:i], nil
		}
	}
	return nil, Incomplete
}

func skip(c *Cursor, n int) error {
	if c.remaining() < n {
		return Incomplete
	}
	_ = c.advance(n)
	return nil
}

// 读取 RESP Integers
func getDecimal(c *Cursor) (int, error) {
	line, err := getLine(c)
	if err != nil {
		return 0, err
	}
	num, err := strconv.ParseInt(string(line), 10, 32)
	if err != nil {
		return 0, errors.New("protocol error; invalid frame format")
	}
	return int(num), nil
}

func peekByte(c *Cursor) (byte, error) {
	if !c.hasRemaining() {
		return 0, Incomplete
	}
	return c.chunk()[0], nil
}

func getByte(c *Cursor) (byte, error) {
	if !c.hasRemaining() {
		return 0, Incomplete
	}
	return c.getByte()
}
