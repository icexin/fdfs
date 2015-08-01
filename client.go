package fdfs

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"net"
)

type TrackerServer struct {
	conn net.Conn
}

func NewTrackerServer(addr string) (*TrackerServer, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &TrackerServer{
		conn: conn,
	}, nil
}

func (t *TrackerServer) GetUploadStoreServer() (group, addr string, idx int, err error) {
	p := new(Packet)
	p.WriteCmd(TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ONE)
	err = p.Encode(t.conn)
	if err != nil {
		return
	}

	p.Reset()

	var info struct {
		Group [FDFS_GROUP_NAME_MAX_LEN]byte
		IP    [IP_ADDRESS_SIZE - 1]byte
		Port  uint64
		Index byte
	}

	err = DecodeBody(p, t.conn, &info)
	if err != nil {
		return
	}

	group = string(info.Group[:])
	addr = fmt.Sprintf("%s:%d", info.IP, info.Port)
	idx = int(info.Index)
	return
}

func (t *TrackerServer) GetDownloadStoreServer() (addr string, err error) {
	p := new(Packet)
	p.WriteCmd(TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE)
	err = p.Encode(t.conn)
	if err != nil {
		return
	}

	p.Reset()

	var info struct {
		Group [FDFS_GROUP_NAME_MAX_LEN]byte
		IP    [IP_ADDRESS_SIZE - 1]byte
		Port  uint64
	}

	err = DecodeBody(p, t.conn, &info)
	if err != nil {
		return
	}

	addr = fmt.Sprintf("%s:%d", info.IP, info.Port)
	return
}

type FileMeta map[string]string

func (m FileMeta) Marshal() []byte {
	if m == nil {
		return make([]byte, 0)
	}

	buf := new(bytes.Buffer)
	for k, v := range m {
		io.WriteString(buf, k)
		buf.WriteByte(2)
		io.WriteString(buf, v)
		buf.WriteByte(1)
	}
	return buf.Bytes()
}

type StoreServer struct {
	conn net.Conn
	rw   *bufio.ReadWriter
}

func NewStoreServer(addr string) (*StoreServer, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &StoreServer{
		conn: conn,
		rw: bufio.NewReadWriter(
			bufio.NewReader(conn),
			bufio.NewWriter(conn),
		),
	}, nil
}

func (s *StoreServer) Upload(idx int, r io.Reader, size int, ext string) (group, path string, err error) {
	p := new(Packet)
	// 构造固定长度的头部信息
	body1 := struct {
		Idx      byte
		FileSize uint64
		Ext      [FDFS_FILE_EXT_NAME_MAX_LEN]byte
	}{
		Idx:      byte(idx),
		FileSize: uint64(size),
	}
	copy(body1.Ext[:], ext)

	p.WriteCmd(STORAGE_PROTO_CMD_UPLOAD_FILE)
	binary.Write(p, binary.BigEndian, &body1)
	p.EncodeAttach(s.rw, r, size)
	err = s.rw.Flush()
	if err != nil {
		return
	}

	p.Reset()

	var info struct {
		Group [FDFS_GROUP_NAME_MAX_LEN]byte
	}
	err = DecodeBody(p, s.rw, &info)
	if err != nil {
		return
	}

	group = string(info.Group[:])

	// read remaining as file name
	var buf []byte
	buf, err = ioutil.ReadAll(p)
	if err != nil {
		return
	}
	path = string(buf)
	return
}

func (s *StoreServer) Download(group, path string, offset, length uint64) (r io.Reader, err error) {
	fixBody := struct {
		Offset uint64
		Length uint64
		Group  [FDFS_GROUP_NAME_MAX_LEN]byte
	}{
		Offset: offset,
		Length: length,
	}
	copy(fixBody.Group[:], group)

	p := new(Packet)
	p.WriteCmd(STORAGE_PROTO_CMD_DOWNLOAD_FILE)
	binary.Write(p, binary.BigEndian, &fixBody)
	io.WriteString(p, path)

	p.Encode(s.rw)
	err = s.rw.Flush()
	if err != nil {
		return
	}

	p.Reset()

	err = p.Decode(s.rw)
	if err != nil {
		return
	}

	return p, nil
}

func (s *StoreServer) Close() error {
	return s.conn.Close()
}
