package cmd

import (
	"gokvs/engines"
	"gokvs/network"
)

type Set struct {
	// the lockup key
	key string
	// the value to be store
	value string
}

func NewSet(key string, value string) Command {
	return &Set{
		key, value,
	}
}

// 将接收到的Frame解析为一个 Set 命令
func parseSetFrame(p *network.Parse) (Command, error) {
	key, err := p.NextString()
	if err != nil {
		return nil, err
	}
	value, err := p.NextString()
	if err != nil {
		return nil, err
	}
	cmd := &Set{
		key, value,
	}
	return cmd, nil
}

// Apply 执行 Set 命令
func (c *Set) Apply(db engines.KvsEngine, connnection network.Connnection) error {
	rsp := new(network.Frame)
	err := db.Set(c.key, c.value)
	if err != nil {
		rsp.Ftype = network.Error
		rsp.Value = err.Error()
	} else {
		rsp.Ftype = network.Simple
		rsp.Value = "OK"
	}
	err = connnection.WriteFrame(rsp)
	if err != nil {
		return err
	}
	return nil
}

// IntoFrame 将command转换为Frame
func (c *Set) IntoFrame() network.Frame {
	array := []*network.Frame{
		{
			Ftype: network.Bulk,
			Value: "set",
		},
		{
			Ftype: network.Bulk,
			Value: c.key,
		},
		{
			Ftype: network.Bulk,
			Value: c.value,
		},
	}
	return network.Frame{
		Ftype: network.Array,
		Value: array,
	}
}

func (c *Set) Name() string {
	return SET
}
