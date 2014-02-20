package mysql

import (
	"bytes"
	"io"
)

type binlogConn struct {
	mc      *mysqlConn
	readBuf bytes.Buffer
}

func ConnectBinlog(dsn string, clientId uint32, filename string, pos uint32, flags uint16) (Binlog, error) {
	dr := MySQLDriver{}
	c, err := dr.Open(dsn)
	if err != nil {
		return nil, err
	}
	mc := c.(*mysqlConn)
	if pos < 4 {
		pos = 4
	}
	err = mc.writeCommandBinlogDump(clientId, filename, pos, flags)
	if err != nil {
		return nil, err
	}
	return &binlogConn{mc: mc}, nil
}

func (b *binlogConn) ReadEvent() ([]byte, error) {
	data, err := b.mc.readPacket()
	if err != nil {
		return nil, err
	}
	if data[0] != iOK {
		// EOF Packet
		if data[0] == iEOF && len(data) == 5 {
			return nil, io.EOF
		}
		// Error otherwise
		return nil, b.mc.handleErrorPacket(data)
	}
	return data[1:], err
}

func (b *binlogConn) Close() error {
	return b.mc.Close()
}
