package fdfs

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

const (
	FDFS_GROUP_NAME_MAX_LEN    = 16
	IP_ADDRESS_SIZE            = 16
	FDFS_FILE_EXT_NAME_MAX_LEN = 6
)

const (
	STORAGE_PROTO_CMD_UPLOAD_FILE                           = 11
	STORAGE_PROTO_CMD_DOWNLOAD_FILE                         = 14
	TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ONE = 101
	TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE               = 102
	TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITH_GROUP_ONE    = 104
)

type Status int

func (s Status) Error() string {
	switch s {
	case 0:
		return "ok"
	default:
		return fmt.Sprintf("status code:%d", s)
	}
}

type PacketHeader struct {
	Len    uint64
	Cmd    uint8
	Status uint8
}

func (h *PacketHeader) Encode(w io.Writer) error {
	return binary.Write(w, binary.BigEndian, h)
}

func (h *PacketHeader) Decode(r io.Reader) error {
	return binary.Read(r, binary.BigEndian, h)
}

type Packet struct {
	PacketHeader

	// for encode
	body bytes.Buffer

	// for decode
	r io.Reader
}

func (p *Packet) Write(b []byte) (int, error) {
	return p.body.Write(b)
}

func (p *Packet) Read(b []byte) (int, error) {
	return p.r.Read(b)
}

func (p *Packet) WriteCmd(cmd uint8) error {
	p.Cmd = cmd
	return nil
}

func (p *Packet) WriteStatus(stat uint8) error {
	p.Status = stat
	return nil
}

func (p *Packet) Encode(w io.Writer) error {
	p.Len = uint64(p.body.Len())
	err := binary.Write(w, binary.BigEndian, &p.PacketHeader)
	if err != nil {
		return err
	}
	_, err = io.Copy(w, &p.body)
	return err
}

func (p *Packet) EncodeAttach(w io.Writer, r io.Reader, size int) error {
	p.Len = uint64(p.body.Len() + size)
	err := binary.Write(w, binary.BigEndian, &p.PacketHeader)
	if err != nil {
		return err
	}
	rr := io.MultiReader(&p.body, r)
	_, err = io.Copy(w, rr)
	return err
}

func (p *Packet) Decode(r io.Reader) error {
	h := &p.PacketHeader
	err := binary.Read(r, binary.BigEndian, h)
	if err != nil {
		return err
	}
	if h.Status != 0 {
		return Status(h.Status)
	}
	p.r = &io.LimitedReader{
		R: r,
		N: int64(p.Len),
	}
	return nil
}

func (p *Packet) Reset() {
	p.body.Reset()
	p.r = nil
	p.Len = 0
	p.Status = 0
	p.Cmd = 0
}

func DecodeBody(p *Packet, r io.Reader, v interface{}) error {
	err := p.Decode(r)
	if err != nil {
		return err
	}
	return binary.Read(p, binary.BigEndian, v)
}

func EncodeBody(p *Packet, w io.Writer, v interface{}) error {
	err := binary.Write(p, binary.BigEndian, v)
	if err != nil {
		return err
	}
	return p.Encode(w)
}

type GroupInfo struct {
	Name               [FDFS_GROUP_NAME_MAX_LEN + 1]byte
	TotalMB            uint64
	FreeMB             uint64
	TrunkFreeMB        uint64
	Count              uint64
	StoragePort        uint64
	StoreHTTPPort      uint64
	ActiveCount        uint64
	CurrWriteServer    uint64
	StorePathCount     uint64
	SubdirCountPerPath uint64
	CurrTrunkFileId    uint64
}
