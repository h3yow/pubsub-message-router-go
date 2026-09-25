package transport

import (
	"encoding/base64"
	"encoding/json"
)

// Encode builds a wire Frame from raw payload bytes, filling in the wire
// version and crc32 checksum used for corruption detection.
func Encode(id, topic, key string, seq uint64, payload []byte) (Frame, error) {
	f := Frame{WireVersion: FrameVersion, ID: id, Topic: topic, Key: key, Seq: seq}
	if len(payload) > 0 {
		f.Payload = base64.StdEncoding.EncodeToString(payload)
		f.CRC32 = crc32IEEE(payload)
	}
	return f, nil
}

// MarshalFrame returns the single-line JSON encoding of f.
func MarshalFrame(f Frame) ([]byte, error) {
	if f.WireVersion == "" {
		f.WireVersion = FrameVersion
	}
	return json.Marshal(f)
}
