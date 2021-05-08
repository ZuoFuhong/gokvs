package cmd

import (
	"errors"
	"gokvs/engines"
	"gokvs/network"
)

const (
	SET    = "set"
	GET    = "get"
	DELETE = "del"
)

type Command interface {
	Apply(db engines.KvsEngine, connnection network.Connnection) error
	IntoFrame() network.Frame
	Name() string
}

// FromFrame 从接收的Frame中解析出command
func FromFrame(frame *network.Frame) (Command, error) {
	parse, err := network.NewParse(frame)
	if err != nil {
		return nil, err
	}
	var cmd Command
	commandName, err := parse.NextString()
	switch commandName {
	case SET:
		cmd, err = parseSetFrame(parse)
	case GET:
		cmd, err = parseGetFrame(parse)
	case DELETE:
		cmd, err = parseDeleteFrame(parse)
	default:
		err = errors.New("unknown command name")
	}
	if err == nil {
		err = parse.Finish()
	}
	return cmd, err
}
