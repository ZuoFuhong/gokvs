package cmd

import (
	"gokvs/engines"
	kvserror "gokvs/errors"
	"gokvs/network"
)

type Get struct {
	// the lockup key
	key string
}

func NewGet(key string) Command {
	return &Get{
		key,
	}
}

// 从接收的Frame中解析一个 Get 命令
func parseGetFrame(parse *network.Parse) (Command, error) {
	key, err := parse.NextString()
	if err != nil {
		return nil, err
	}
	cmd := &Get{
		key,
	}
	return cmd, nil
}

func (c *Get) Apply(db engines.KvsEngine, connnection network.Connnection) error {
	rsp := new(network.Frame)
	value, err := db.Get(c.key)
	if err != nil {
		if err == kvserror.KeyNotFound {
			rsp.Ftype = network.Null
		} else {
			rsp.Ftype = network.Error
			rsp.Value = err.Error()
		}
	} else {
		rsp.Ftype = network.Bulk
		rsp.Value = value
	}
	err = connnection.WriteFrame(rsp)
	if err != nil {
		return err
	}
	return nil
}

func (c *Get) IntoFrame() network.Frame {
	array := []*network.Frame{
		{
			Ftype: network.Bulk,
			Value: "get",
		},
		{
			Ftype: network.Bulk,
			Value: c.key,
		},
	}
	return network.Frame{
		Ftype: network.Array,
		Value: array,
	}
}

func (c *Get) Name() string {
	return GET
}
