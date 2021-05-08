package gokvs

import (
	"errors"
	"gokvs/cmd"
	"gokvs/network"
	"net"
	"strconv"
)

type Client struct {
	connnection network.Connnection
}

func NewClient(addr string) (*Client, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return nil, err
	}
	client := &Client{
		connnection: network.NewConnection(conn),
	}
	return client, nil
}

func (c *Client) Set(key, value string) (string, error) {
	frame := cmd.NewSet(key, value).IntoFrame()
	err := c.connnection.WriteFrame(&frame)
	if err != nil {
		return "", err
	}
	rsp, err := c.readResponse()
	if err != nil {
		return "", err
	}
	switch rsp.Ftype {
	case network.Simple:
		return rsp.Value.(string), nil
	case network.Error:
		return rsp.Value.(string), nil
	default:
		return "", errors.New("protocol error; expected simple frame or error frame")
	}
}

func (c *Client) Get(key string) (string, error) {
	frame := cmd.NewGet(key).IntoFrame()
	err := c.connnection.WriteFrame(&frame)
	if err != nil {
		return "", err
	}
	rsp, err := c.readResponse()
	if err != nil {
		return "", err
	}
	switch rsp.Ftype {
	case network.Bulk:
		return rsp.Value.(string), nil
	case network.Null:
		return "null", nil
	case network.Error:
		return rsp.Value.(string), nil
	default:
		return "", errors.New("protocol error; expected simple frame or error frame")
	}
}

func (c *Client) Del(key string) (string, error) {
	frame := cmd.NewDelete(key).IntoFrame()
	err := c.connnection.WriteFrame(&frame)
	if err != nil {
		return "", err
	}
	rsp, err := c.readResponse()
	if err != nil {
		return "", err
	}
	switch rsp.Ftype {
	case network.Integer:
		return strconv.FormatInt(int64(rsp.Value.(int)), 10), nil
	case network.Error:
		return rsp.Value.(string), nil
	default:
		return "", errors.New("protocol error; expected simple frame or error frame")
	}
}

func (c *Client) readResponse() (*network.Frame, error) {
	frame, err := c.connnection.ReadFrame()
	if err != nil {
		return nil, err
	}
	return frame, nil
}
