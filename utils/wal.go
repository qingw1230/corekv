package utils

import (
	"bytes"
	"encoding/binary"
	"hash"
	"hash/crc32"
	"io"
)

type LogEntry func(e *Entry, vp *ValuePtr) error

type WalHeader struct {
	KeyLen    uint32
	ValueLen  uint32
	Meta      byte
	ExpiresAt uint64
}

const maxHeaderSize int = 21

// Encode 将 WalHeader 编码进 out
func (h *WalHeader) Encode(out []byte) int {
	index := 0
	index = binary.PutUvarint(out[index:], uint64(h.KeyLen))
	index += binary.PutUvarint(out[index:], uint64(h.ValueLen))
	index += binary.PutUvarint(out[index:], uint64(h.Meta))
	index += binary.PutUvarint(out[index:], h.ExpiresAt)
	return index
}

// Decode 从 reader 解码出 WalHead 并写入 h
func (h *WalHeader) Decode(reader *HashReader) (int, error) {
	var err error
	klen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.KeyLen = uint32(klen)
	vlen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.ValueLen = uint32(vlen)
	meta, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.Meta = byte(meta)
	h.ExpiresAt, err = binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	return reader.BytesRead, nil
}

// WalCodec 将 entry 编码后写入 buf
// WAL 格式    | header | key | value | checksum |
// header 格式 | KeyLen | ValueLen | Meta | ExpiresAt |
func WalCodec(buf *bytes.Buffer, e *Entry) int {
	buf.Reset()
	h := WalHeader{
		KeyLen:    uint32(len(e.Key)),
		ValueLen:  uint32(len(e.Value)),
		ExpiresAt: e.ExpiresAt,
	}

	hash := crc32.New(CastagnoliCrcTable)
	writer := io.MultiWriter(buf, hash)

	var headerEnc [maxHeaderSize]byte
	sz := h.Encode(headerEnc[:])
	Panic2(writer.Write(headerEnc[:sz]))
	Panic2(writer.Write(e.Key))
	Panic2(writer.Write(e.Value))

	var crcBuf [crc32.Size]byte
	binary.BigEndian.PutUint32(crcBuf[:], hash.Sum32())
	Panic2(writer.Write(crcBuf[:]))
	return len(headerEnc[:sz]) + len(e.Key) + len(e.Value) + len(crcBuf)
}

func EstimateWalCodecSize(e *Entry) int {
	return len(e.Key) + len(e.Value) + 8 + crc32.Size + maxHeaderSize
}

func (e *Entry) IsZero() bool {
	return len(e.Key) == 0
}

func (e *Entry) LogHeaderLen() int {
	return e.Hlen
}

func (e *Entry) LogOffset() uint32 {
	return e.Offset
}

type HashReader struct {
	R         io.Reader
	H         hash.Hash32
	BytesRead int
}

func NewHashReader(r io.Reader) *HashReader {
	hash := crc32.New(CastagnoliCrcTable)
	return &HashReader{
		R: r,
		H: hash,
	}
}

func (h *HashReader) Read(p []byte) (int, error) {
	n, err := h.R.Read(p)
	if err != nil {
		return n, err
	}
	h.BytesRead += n
	return h.H.Write(p[:n])
}

func (h *HashReader) ReadByte() (byte, error) {
	b := make([]byte, 1)
	_, err := h.Read(b)
	return b[0], err
}

func (h *HashReader) Sum32() uint32 {
	return h.H.Sum32()
}
