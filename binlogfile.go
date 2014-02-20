package mysql

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path"
	"time"
)

func OpenBinlog(filename string, pos uint32, flags uint16) (Binlog, error) {
	file, err := openBinlogFile(filename)
	if err != nil {
		return nil, err
	}
	return &binlogFile{
		file:  file,
		name:  path.Base(filename),
		dir:   path.Dir(filename),
		flags: flags,
		spos:  pos,
		pos:   4,
	}, nil
}

type binlogFile struct {
	file   *os.File
	name   string
	dir    string
	flags  uint16
	spos   uint32
	pos    uint32
	rotate bool
}

func openBinlogFile(name string) (*os.File, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	var h [4]byte
	if _, err = file.Read(h[:]); err != nil {
		return nil, err
	}
	if h != [4]byte{0xFE, 'b', 'i', 'n'} {
		return nil, errors.New("not binlog")
	}
	return file, nil
}

func (b *binlogFile) readHeader() (h [19]byte, size uint32, pos uint32, err error) {
	for {
		_, err = b.file.Read(h[:])
		if err != io.EOF {
			break
		}
		if b.flags == BinlogNonBlock {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		return
	}
	size = binary.LittleEndian.Uint32(h[9:13])
	pos = binary.LittleEndian.Uint32(h[13:17])
	if pos < b.pos || b.pos+size != pos {
		return [19]byte{}, 0, 0, errors.New("bad event")
	}
	return
}

func (b *binlogFile) ReadEvent() ([]byte, error) {
	if b.rotate {
		file, err := openBinlogFile(b.dir + "/" + b.name)
		if err != nil {
			return nil, err
		}
		b.file.Close()
		b.file = file
		b.rotate = false
	}

	h, size, pos, err := b.readHeader()
	if err != nil {
		return nil, err
	}
	buf := make([]byte, size)
	copy(buf[:19], h[:])

	if size > 19 {
		_, err := b.file.Read(buf[19:])
		if err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return nil, err
		}
	}

	if b.spos > pos {
		_, err := b.file.Seek(int64(b.spos), 0)
		if err != nil {
			return nil, err
		}
		b.pos = b.spos
	} else if buf[4] == binlogRotateEvent && len(buf) > 27 {
		b.name = string(buf[27:])
		b.pos = uint32(binary.LittleEndian.Uint64(buf[19:27]))
		b.rotate = true
	} else {
		b.pos = pos
	}
	return buf, nil
}

func (b *binlogFile) Close() error {
	return b.file.Close()
}
