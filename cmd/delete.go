package cmd

import (
	"gokvs/engines"
	kvserror "gokvs/errors"
	"gokvs/network"
)

type Delete struct {
	key string
}

func NewDelete(key string) Command {
	return &Delete{
		key,
	}
}

// 从接收的Frame中解析一个 Delete 命令
func parseDeleteFrame(parse *network.Parse) (Command, error) {
	key, err := parse.NextString()
	if err != nil {
		return nil, err
	}
	cmd := &Delete{
		key,
	}
	return cmd, nil
}

func (c *Delete) Apply(db engines.KvsEngine, connnection network.Connnection) error {
	rsp := new(network.Frame)
	err := db.Remove(c.key)
	if err != nil {
		if err == kvserror.KeyNotFound {
			rsp.Ftype = network.Integer
			rsp.Value = 0
		} else {
			rsp.Ftype = network.Error
			rsp.Value = err.Error()
		}
	} else {
		rsp.Ftype = network.Integer
		rsp.Value = 1
	}
	err = connnection.WriteFrame(rsp)
	if err != nil {
		return err
	}
	return nil
}

func (c *Delete) IntoFrame() network.Frame {
	array := []*network.Frame{
		{
			Ftype: network.Bulk,
			Value: "del",
		},
		{
			Ftype: network.Bulk,
			Value: c.key,
		},
	}
	frame := network.Frame{
		Ftype: network.Array,
		Value: array,
	}
	return frame
}

func (c *Delete) Name() string {
	return DELETE
}
