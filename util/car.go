package util

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car"
)

// WriteCarHeader writes the header to the writer and returns the number of bytes written.
func WriteCarHeader(w io.Writer, header car.CarHeader) (int, error) {
	headerBuf := new(bytes.Buffer)
	err := car.WriteHeader(&header, headerBuf)
	if err != nil {
		return 0, err
	}
	return headerBuf.Len(), nil
}

// WriteBlock writes the block to the writer and returns the number of bytes written as well as the offset to the actual bytes
func WriteBlock(w io.Writer, c cid.Cid, bytes []byte) (int, int, error) {
	sum := uint64(len(bytes) + c.ByteLen())
	buf := make([]byte, 8)
	n := binary.PutUvarint(buf, sum)
	_, err := w.Write(buf[:n])
	if err != nil {
		return 0, 0, err
	}
	_, err = w.Write(c.Bytes())
	if err != nil {
		return 0, 0, err
	}
	_, err = w.Write(bytes)
	if err != nil {
		return 0, 0, err
	}
	return n + c.ByteLen() + len(bytes), n + c.ByteLen(), nil
}
