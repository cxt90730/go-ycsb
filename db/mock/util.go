package mock

import (
	"github.com/journeymidnight/radoshttpd/rados"
	"os"
	"errors"
	"net/http"
	"io/ioutil"
)

func newCephConn(path string) (*rados.Conn, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if fi.IsDir() {
		return nil, errors.New("path can not be a directory")
	}
	d, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer d.Close()

	conn, err := rados.NewConn("admin")
	conn.SetConfigOption("rados_mon_op_timeout", MON_TIMEOUT)
	conn.SetConfigOption("rados_osd_op_timeout", OSD_TIMEOUT)
	err = conn.ReadConfigFile(path)
	if err != nil {
		return nil, err
	}

	err = conn.Connect()
	if err != nil {
		return nil, err
	}
	return conn, nil
}

type myHandler struct {
	*rados.Conn
}

func (m *myHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	pool := r.Header.Get("Pool")
	oid := r.Header.Get("Oid")
	mockType := r.Header.Get("Type")
	if mockType == "1" || mockType == "2" {
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(400)
			w.Write([]byte("hehe read data"))
			return
		}
		defer r.Body.Close()
		if mockType == "2" {
			conn := m.Conn
			if err != nil {
				w.WriteHeader(400)
				w.Write([]byte("hehe ceph conn"))
				return
			}
			p, err := conn.OpenPool(pool)
			if err != nil {
				w.WriteHeader(400)
				w.Write([]byte("hehe open pool"))
				return
			}
			err = p.WriteSmallObject(oid, data)
			if err != nil {
				w.WriteHeader(400)
				w.Write([]byte("hehe WriteSmallObject"))
				return
			}
		}

	}

	w.Write(nil)
}
