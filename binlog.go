package mysql

import (
	"database/sql/driver"
	"io"
)

const (
	binlogStartEventV3 byte = 0x01 + iota
	binlogQueryEvent
	binlogStopEvent
	binlogRotateEvent

	binlogFormatDescriptionEvent = 0x0F
	binlogXidEvent               = 0x10
)

// MySQL 5.1 - 5.5
const (
	binlogTableMapEvent byte = 0x13 + iota
	binlogWriteRowsEventV0
	binlogUpdateRowsEventV0
	binlogDeleteRowsEventV0
	binlogWriteRowsEventV1
	binlogUpdateRowsEventV1
	binlogDeleteRowsEventV1
)

// MySQL 5.6+
const (
	binlogWriteRowsEventV2 byte = 0x1E + iota
	binlogUpdateRowsEventV2
	binlogDeleteRowsEventV2
)

func (mc *mysqlConn) writeCommandBinlogDump(filename string, pos uint32, serverId uint32, flags uint16) error {
	// Reset Packet Sequence
	mc.sequence = 0

	pktLen := 1 + 4 + 2 + 4 + len(filename)
	data := mc.buf.takeSmallBuffer(pktLen + 4)
	if data == nil {
		// can not take the buffer. Something must be wrong with the connection
		errLog.Print(errBusyBuffer)
		return driver.ErrBadConn
	}

	// Add command byte
	data[4] = comBinlogDump

	// Add pos [32 bit]
	data[5] = byte(pos)
	data[6] = byte(pos >> 8)
	data[7] = byte(pos >> 16)
	data[8] = byte(pos >> 24)
	// Add flags [16 bit]
	data[9] = byte(flags)
	data[10] = byte(flags >> 8)
	// Add serverId [32 bit]
	data[11] = byte(serverId)
	data[12] = byte(serverId >> 8)
	data[13] = byte(serverId >> 16)
	data[14] = byte(serverId >> 24)

	// Add filename
	copy(data[15:], filename)

	// Send CMD packet
	return mc.writePacket(data)
}

type Binlog struct {
	mc  *mysqlConn
	buf []byte
}

func OpenBinlog(dsn string, filename string, pos uint32, serverId uint32) (*Binlog, error) {
	dr, err := new(MySQLDriver).Open(dsn)
	if err != nil {
		return nil, err
	}
	mc := dr.(*mysqlConn)
	// TODO: Constant for flags
	err = mc.writeCommandBinlogDump(filename, pos, serverId, 0)
	if err != nil {
		return nil, err
	}
	return &Binlog{mc: mc}, nil
}

func (b *Binlog) Read(p []byte) (n int, err error) {
	var data []byte
	if len(b.buf) > 0 {
		data = b.buf
		b.buf = nil
	} else {
		data, err = b.ReadEvent()
		if err != nil {
			return 0, err
		}
	}
	if len(data) > len(p) {
		b.buf = data[len(p):]
		data = data[:len(p)]
	}
	copy(p[:len(data)], data)
	return len(data), nil
}

func (b *Binlog) ReadEvent() ([]byte, error) {
	data, err := b.mc.readPacket()
	if err != nil {
		return nil, err
	}
	b.buf = nil
	if data[0] != iOK {
		// EOF Packet
		if data[0] == iEOF && len(data) == 5 {
			return nil, io.EOF
		}
		// Error otherwise
		return nil, b.mc.handleErrorPacket(data)
	}
	return data[1:], nil
}

// TODO: Decide what to return.
func (b *Binlog) NextRow() error {
	for {
		data, err := b.ReadEvent()
		if err != nil {
			return err
		}
		switch data[4] {
		case binlogTableMapEvent:
			// TODO: Remember column types for upcoming Rows Event
			println("table map")
		case binlogWriteRowsEventV0, binlogWriteRowsEventV1, binlogWriteRowsEventV2:
			println("write")
		case binlogUpdateRowsEventV0, binlogUpdateRowsEventV1, binlogUpdateRowsEventV2:
			println("update")
		case binlogDeleteRowsEventV0, binlogDeleteRowsEventV1, binlogDeleteRowsEventV2:
			println("delete")
		}
	}
}

func (b *Binlog) Close() error {
	return b.mc.Close()
}
