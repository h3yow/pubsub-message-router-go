package pubsub

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// 磁盘记录格式（小端）：
//
//	magic[4]="PSMJ" | version u8 | seq u64(BE)
//	ulen u32 | ubytes    （u = protocol/topic/key，依次各一组）
//	plen u32 | pbytes    （payload）
//	crc u32              （对除 magic 与 crc 自身外全部字节的 IEEE CRC32）
//
// 任何截断或校验失败都视为日志尾部损坏：恢复时停在该处并截断，
// 绝不静默忽略中间记录。
const journalMagic = "PSMJ"
const journalVersion byte = 1

// CorruptionError 描述恢复时发现的日志损坏位置，便于调用方解释。
type CorruptionError struct {
	Offset int64
	Detail string
}

func (e *CorruptionError) Error() string {
	return fmt.Sprintf("journal corruption at offset %d: %s", e.Offset, e.Detail)
}

// Journal 是按顺序键追加的持久化日志。
// 它服务于两个目标：重启后恢复每个 Key 的去重/顺序高水位，以及对
// 崩溃前已接受消息进行重放（配合新订阅者可验证“不丢、不重”）。
type Journal struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer

	highWater map[string]uint64
}

// OpenJournal 打开（或创建）追加日志并执行校验恢复。
// 若发现尾部损坏，会截断到最后一条有效记录之后并返回 *CorruptionError
// （错误可被 errors.As 识别）；此时日志仍处于可用状态。
func OpenJournal(path string) (*Journal, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open journal: %w", err)
	}
	j := &Journal{f: f, highWater: make(map[string]uint64)}

	validEnd, corrErr := j.recover()
	if corrErr != nil {
		// 截断损坏尾部，保证后续追加不会污染解析。
		if terr := f.Truncate(validEnd); terr != nil {
			f.Close()
			return nil, fmt.Errorf("truncate corrupt journal tail: %w (caused by: %v)", terr, corrErr)
		}
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, fmt.Errorf("seek journal end: %w", err)
	}
	j.w = bufio.NewWriter(f)
	return j, corrErr
}

// recover 顺序读取全部记录，重建高水位；返回最后有效记录结束偏移与损坏错误。
func (j *Journal) recover() (int64, error) {
	if _, err := j.f.Seek(0, io.SeekStart); err != nil {
		return 0, fmt.Errorf("seek journal start: %w", err)
	}
	r := bufio.NewReader(j.f)
	var offset int64

	for {
		start := offset
		magic := make([]byte, 4)
		n, err := io.ReadFull(r, magic)
		if errors.Is(err, io.EOF) && n == 0 {
			return offset, nil // 干净结束
		}
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return start, &CorruptionError{Offset: start, Detail: "truncated magic header"}
		}
		if err != nil {
			return start, fmt.Errorf("read journal: %w", err)
		}
		offset += int64(n)
		if string(magic) != journalMagic {
			return start, &CorruptionError{Offset: start, Detail: "bad magic"}
		}

		rec, err := readRecord(r)
		if err != nil {
			return start, &CorruptionError{Offset: start, Detail: err.Error()}
		}
		offset += int64(rec.bytesRead)

		if rec.msg.Seq > j.highWater[rec.msg.Key] {
			j.highWater[rec.msg.Key] = rec.msg.Seq
		}
	}
}

type recoveredRecord struct {
	msg       *Message
	bytesRead int64
}

func readRecord(r *bufio.Reader) (*recoveredRecord, error) {
	read := 4 // magic 已由调用方消费
	hdr := make([]byte, 9)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return nil, fmt.Errorf("truncated fixed header: %v", err)
	}
	read += 9
	if hdr[0] != journalVersion {
		return nil, fmt.Errorf("unsupported journal record version: %d", hdr[0])
	}
	seq := binary.BigEndian.Uint64(hdr[1:9])

	readField := func() (string, error) {
		var lenBuf [4]byte
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			return "", fmt.Errorf("truncated length prefix: %v", err)
		}
		n := binary.BigEndian.Uint32(lenBuf[:])
		buf := make([]byte, n)
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", fmt.Errorf("truncated field of %d bytes: %v", n, err)
		}
		read += 4 + int(n)
		return string(buf), nil
	}
	protocol, err := readField()
	if err != nil {
		return nil, err
	}
	topic, err := readField()
	if err != nil {
		return nil, err
	}
	key, err := readField()
	if err != nil {
		return nil, err
	}

	var plenBuf [4]byte
	if _, err := io.ReadFull(r, plenBuf[:]); err != nil {
		return nil, fmt.Errorf("truncated payload length: %v", err)
	}
	read += 4
	plen := binary.BigEndian.Uint32(plenBuf[:])
	payload := make([]byte, plen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, fmt.Errorf("truncated payload: %v", err)
	}
	read += int(plen)

	var crcBuf [4]byte
	if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
		return nil, fmt.Errorf("truncated crc: %v", err)
	}
	read += 4

	msg := &Message{Protocol: protocol, Topic: topic, Key: key, Seq: seq, Payload: payload}
	wantCRC := binary.BigEndian.Uint32(crcBuf[:])
	gotCRC := crcRecord(hdr, protocol, topic, key, payload)
	if wantCRC != gotCRC {
		return nil, fmt.Errorf("crc mismatch: stored=%08x computed=%08x", wantCRC, gotCRC)
	}
	return &recoveredRecord{msg: msg, bytesRead: int64(read)}, nil
}

func crcRecord(hdr []byte, protocol, topic, key string, payload []byte) uint32 {
	h := crc32.NewIEEE()
	h.Write(hdr)
	writeStringCRC := func(s string) {
		var lb [4]byte
		binary.BigEndian.PutUint32(lb[:], uint32(len(s)))
		h.Write(lb[:])
		h.Write([]byte(s))
	}
	writeStringCRC(protocol)
	writeStringCRC(topic)
	writeStringCRC(key)
	var lb [4]byte
	binary.BigEndian.PutUint32(lb[:], uint32(len(payload)))
	h.Write(lb[:])
	h.Write(payload)
	return h.Sum32()
}

// Append 追加一条已接受的消息并刷盘（fsync），保证崩溃后“已接受”不丢。
func (j *Journal) Append(m *Message) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	hdr := make([]byte, 9)
	hdr[0] = journalVersion
	binary.BigEndian.PutUint64(hdr[1:9], m.Seq)

	if _, err := j.w.WriteString(journalMagic); err != nil {
		return err
	}
	if _, err := j.w.Write(hdr); err != nil {
		return err
	}
	writeField := func(s string) error {
		var lb [4]byte
		binary.BigEndian.PutUint32(lb[:], uint32(len(s)))
		if _, err := j.w.Write(lb[:]); err != nil {
			return err
		}
		_, err := j.w.WriteString(s)
		return err
	}
	if err := writeField(m.Protocol); err != nil {
		return err
	}
	if err := writeField(m.Topic); err != nil {
		return err
	}
	if err := writeField(m.Key); err != nil {
		return err
	}
	var lb [4]byte
	binary.BigEndian.PutUint32(lb[:], uint32(len(m.Payload)))
	if _, err := j.w.Write(lb[:]); err != nil {
		return err
	}
	if _, err := j.w.Write(m.Payload); err != nil {
		return err
	}
	crc := crcRecord(hdr, m.Protocol, m.Topic, m.Key, m.Payload)
	var cb [4]byte
	binary.BigEndian.PutUint32(cb[:], crc)
	if _, err := j.w.Write(cb[:]); err != nil {
		return err
	}
	if err := j.w.Flush(); err != nil {
		return err
	}
	return j.f.Sync()
}

// HighWater 返回某顺序键的最大已提交序号（启动恢复 + 运行期追加共同维护）。
func (j *Journal) HighWater(key string) (uint64, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	seq, ok := j.highWater[key]
	return seq, ok
}

// Replay 按提交顺序重放全部已持久化消息。
func (j *Journal) Replay(fn func(*Message) error) error {
	j.mu.Lock()
	path := j.f.Name()
	j.mu.Unlock()

	// 以只读句柄顺序扫描，避免与追加偏移互相干扰。
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open journal for replay: %w", err)
	}
	defer f.Close()
	r := bufio.NewReader(f)
	for {
		magic := make([]byte, 4)
		n, err := io.ReadFull(r, magic)
		if errors.Is(err, io.EOF) && n == 0 {
			return nil
		}
		if err != nil {
			return fmt.Errorf("replay read magic: %w", err)
		}
		if string(magic) != journalMagic {
			return &CorruptionError{Detail: "bad magic during replay"}
		}
		rec, err := readRecord(r)
		if err != nil {
			return &CorruptionError{Detail: err.Error()}
		}
		if err := fn(rec.msg); err != nil {
			return err
		}
	}
}

// Close 关闭日志。
func (j *Journal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.w != nil {
		if err := j.w.Flush(); err != nil {
			return err
		}
	}
	return j.f.Close()
}

var _ io.Closer = (*Journal)(nil)
