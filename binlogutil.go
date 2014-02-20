package mysql

import (
	"database/sql/driver"
)

func (mc *mysqlConn) writeCommandBinlogDump(clientId uint32, filename string, pos uint32, flags uint16) error {
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
	// Add clientId [32 bit]
	data[11] = byte(clientId)
	data[12] = byte(clientId >> 8)
	data[13] = byte(clientId >> 16)
	data[14] = byte(clientId >> 24)

	// Add filename
	copy(data[15:], filename)

	// Send CMD packet
	return mc.writePacket(data)
}
