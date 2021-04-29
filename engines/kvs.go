package engines

import (
	"bufio"
	"encoding/json"
	"fmt"
	kvserror "gokvs/errors"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const CompactionThreshold = 1024 * 1024

type KvsStore struct {
	path        string
	readers     map[uint64]*BufReaderWithPos
	writer      *BufWriterWithPos
	currentGen  uint64
	index       *sync.Map
	uncompacted uint64
}

func NewKvsStore(path string) (KvsEngine, error) {
	genList, err := sortedGenList(path)
	if err != nil {
		return nil, err
	}
	index := &sync.Map{}
	readers := make(map[uint64]*BufReaderWithPos)
	var uncompacted uint64
	for _, gen := range genList {
		file, err := os.Open(logPath(path, uint64(gen)))
		if err != nil {
			return nil, err
		}
		reader := NewBufReaderWithPos(file)
		n, err := load(uint64(gen), reader, index)
		if err != nil {
			return nil, err
		}
		uncompacted += n
		readers[uint64(gen)] = reader
	}
	currentGen := 1
	if len(genList) > 0 {
		currentGen = genList[len(genList)-1] + 1
	}
	writer, err := newLogFile(path, uint64(currentGen))
	if err != nil {
		return nil, err
	}
	kvsStore := &KvsStore{
		path,
		readers,
		writer,
		uint64(currentGen),
		index,
		uncompacted,
	}
	return kvsStore, nil
}

func newLogFile(path string, gen uint64) (*BufWriterWithPos, error) {
	file, err := os.Create(logPath(path, gen))
	if err != nil {
		return nil, err
	}
	writer := NewBufWriterWithPos(file)
	return writer, nil
}

func load(gen uint64, reader *BufReaderWithPos, index *sync.Map) (uint64, error) {
	var uncompacted, pos uint64
	buf := bufio.NewReader(reader.file)
	for bytes, err := buf.ReadSlice('}'); err == nil; bytes, err = buf.ReadSlice('}') {
		newPos := pos + uint64(len(bytes))
		cmd := &Command{}
		err := json.Unmarshal(bytes, cmd)
		if err != nil {
			return 0, err
		}
		if cmd.Type == SET {
			if val, ok := index.Load(cmd.Key); ok {
				uncompacted += val.(*CommandPos).len
			}
			cmdPos := NewCommandPos(gen, pos, newPos)
			index.Store(cmd.Key, cmdPos)
		}
		if cmd.Type == DELETE {
			if val, ok := index.Load(cmd.Key); ok {
				uncompacted += val.(*CommandPos).len
				index.Delete(cmd.Key)
			}
			// Remove命令在下一次压缩中删除，因此将长度置为未压缩
			uncompacted += newPos - pos
		}
		pos = newPos
	}
	return uncompacted, nil
}

func logPath(path string, gen uint64) string {
	return fmt.Sprintf("%s/%d.log", path, gen)
}

func sortedGenList(path string) ([]int, error) {
	genList := make([]int, 0)
	files, err := os.ReadDir(path)
	if err != nil {
		return genList, err
	}
	for _, v := range files {
		if strings.HasSuffix(v.Name(), ".log") {
			tempArr := strings.Split(v.Name(), ".")
			if len(tempArr) != 2 {
				continue
			}
			seq, err := strconv.ParseUint(tempArr[0], 10, 31)
			if err != nil {
				continue
			}
			genList = append(genList, int(seq))
		}
	}
	sort.Ints(genList)
	return genList, nil
}

type CommandType string

const (
	SET    CommandType = "SET"
	DELETE CommandType = "DELETE"
)

type Command struct {
	Type  CommandType `json:"type"`
	Key   string      `json:"key"`
	Value string      `json:"value,omitempty"`
}

type CommandPos struct {
	gen uint64
	pos uint64
	len uint64
}

func NewCommandPos(gen, start, end uint64) *CommandPos {
	pos := &CommandPos{
		gen, start, end - start,
	}
	return pos
}

func (kvs *KvsStore) Set(key, value string) error {
	cmd := &Command{SET, key, value}
	pos := kvs.writer.pos
	bytes, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	err = kvs.writer.write(bytes)
	if err != nil {
		return err
	}
	err = kvs.writer.flush()
	if err != nil {
		return err
	}
	if val, ok := kvs.index.Load(key); ok {
		// 记录重复的命令字节数
		kvs.uncompacted += val.(*CommandPos).len
	} else {
		commandPos := NewCommandPos(kvs.currentGen, pos, kvs.writer.pos)
		kvs.index.Store(key, commandPos)
	}
	if kvs.uncompacted > CompactionThreshold {
		_ = kvs.compact()
	}
	return nil
}

func (kvs *KvsStore) Get(key string) (string, error) {
	if val, ok := kvs.index.Load(key); ok {
		pos := val.(*CommandPos)
		reader := kvs.readers[pos.gen]
		if reader == nil {
			file, err := os.Open(logPath(kvs.path, pos.gen))
			if err != nil {
				return "", err
			}
			reader = NewBufReaderWithPos(file)
			kvs.readers[pos.gen] = reader
		}
		err := reader.seek(pos.pos)
		if err != nil {
			return "", err
		}
		cmd, err := reader.readCommand(pos)
		if err != nil {
			return "", err
		}
		return cmd.Value, nil
	} else {
		return "", kvserror.KeyNotFound
	}
}

func (kvs *KvsStore) Remove(key string) error {
	if _, ok := kvs.index.Load(key); ok {
		cmd := &Command{DELETE, key, ""}
		pos := kvs.writer.pos
		bytes, err := json.Marshal(cmd)
		if err != nil {
			return err
		}
		err = kvs.writer.write(bytes)
		if err != nil {
			return err
		}
		err = kvs.writer.flush()
		if err != nil {
			return err
		}
		kvs.index.Delete(key)
		// Remove命令在下一次压缩中删除，因此将长度置为未压缩
		kvs.uncompacted += kvs.writer.pos - pos
		if kvs.uncompacted > CompactionThreshold {
			_ = kvs.compact()
		}
	} else {
		return kvserror.KeyNotFound
	}
	return nil
}

func (kvs *KvsStore) compact() (err error) {
	compactionGen := kvs.currentGen + 1
	kvs.currentGen += 2
	kvs.writer, err = newLogFile(kvs.path, kvs.currentGen)
	if err != nil {
		return err
	}
	compactionWriter, err := newLogFile(kvs.path, compactionGen)
	if err != nil {
		return err
	}
	kvs.index.Range(func(key, value interface{}) bool {
		cmdPos := value.(CommandPos)
		reader := kvs.readers[cmdPos.gen]
		if reader.pos != cmdPos.pos {
			err = reader.seek(cmdPos.pos)
			if err != nil {
				return false
			}
		}
		buf := make([]byte, cmdPos.len)
		err = reader.read(buf)
		if err != nil {
			return false
		}
		err = compactionWriter.write(buf)
		if err != nil {
			return false
		}
		return true
	})
	err = compactionWriter.flush()
	if err != nil {
		return err
	}

	// remove stale log files
	genList, err := sortedGenList(kvs.path)
	if err != nil {
		return err
	}
	for _, gen := range genList {
		err := os.Remove(logPath(kvs.path, uint64(gen)))
		if err != nil {
			return err
		}
	}
	kvs.uncompacted = 0
	return nil
}

type BufReaderWithPos struct {
	file *os.File
	pos  uint64
}

func NewBufReaderWithPos(file *os.File) *BufReaderWithPos {
	reader := &BufReaderWithPos{
		file: file,
		pos:  0,
	}
	return reader
}

func (r *BufReaderWithPos) readCommand(commandPos *CommandPos) (*Command, error) {
	buf := make([]byte, commandPos.len)
	err := r.read(buf)
	if err != nil {
		return nil, err
	}
	cmd := &Command{}
	err = json.Unmarshal(buf, cmd)
	if err != nil {
		return nil, err
	}
	return cmd, nil
}

func (r *BufReaderWithPos) read(buf []byte) error {
	n, err := r.file.Read(buf)
	if err != nil {
		return err
	}
	r.pos += uint64(n)
	return nil
}

func (r *BufReaderWithPos) seek(pos uint64) error {
	n, err := r.file.Seek(int64(pos), io.SeekStart)
	r.pos = uint64(n)
	return err
}

type BufWriterWithPos struct {
	buf *bufio.Writer
	pos uint64
}

func (w *BufWriterWithPos) write(p []byte) error {
	n, err := w.buf.Write(p)
	if err != nil {
		return err
	}
	w.pos += uint64(n)
	return nil
}

func (w *BufWriterWithPos) flush() error {
	return w.buf.Flush()
}

func NewBufWriterWithPos(file *os.File) *BufWriterWithPos {
	writer := &BufWriterWithPos{
		buf: bufio.NewWriter(file),
		pos: 0,
	}
	return writer
}
