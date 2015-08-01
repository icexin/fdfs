package fdfs

import (
	"io/ioutil"
	"strings"
	"testing"
)

func TestUpload(t *testing.T) {
	s, err := NewTrackerServer("192.168.14.134:22122")
	if err != nil {
		t.Fatal(err)
	}
	group, addr, idx, err := s.GetUploadStoreServer()
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("group:%s addr:%s", group, addr)

	stor, err := NewStoreServer(addr)
	if err != nil {
		t.Fatal(err)
	}

	body := "hello"
	file := strings.NewReader(body)
	grp, path, err := stor.Upload(idx, file, len(body), "")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%s/%s", grp, path)

	r, err := stor.Download(grp, path, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	buf, _ := ioutil.ReadAll(r)
	t.Logf("resp:%s", buf)
}
