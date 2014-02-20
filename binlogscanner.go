package mysql

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

const (
	BinlogOpInsert uint8 = iota
	BinlogOpUpdate
	BinlogOpDelete
)

type BinlogScanner struct {
	b           Binlog
	filters     []string
	loc         *time.Location
	headerSizes []byte
	tables      []tableMap
	table       tableMap
	rows        rowsEvent
	row         []interface{}
	err         error
}

func NewBinlogScanner(b Binlog, filters []string, loc *time.Location) *BinlogScanner {
	if loc == nil {
		loc = time.UTC
	}
	return &BinlogScanner{
		b:       b,
		filters: filters,
		loc:     loc,
		tables:  make([]tableMap, 0, 2),
	}
}

func (s *BinlogScanner) Next() bool {
	if !s.nextRow() {
		if !s.nextRowsEvent() || !s.nextRow() {
			return false
		}
	}
	return true
}

func (s *BinlogScanner) Op() uint8 {
	return s.rows.op
}

func (s *BinlogScanner) Database() string {
	return s.table.database
}

func (s *BinlogScanner) Table() string {
	return s.table.table
}

func (s *BinlogScanner) Scan(dest ...interface{}) error {
	if s.row == nil {
		return errors.New("mysql: Scan called without calling Next")
	}
	if len(dest) != len(s.row) {
		return fmt.Errorf("mysql: expected %d destination arguments in Scan, not %d", s.rows.columns, len(s.row))
	}
	for i, v := range s.row {
		err := convertAssign(dest[i], v)
		if err != nil {
			return fmt.Errorf("mysql: Scan error on column index %d: %v", i, err)
		}
	}
	return nil
}

func (s *BinlogScanner) nextRowsEvent() bool {
	if s.rows.flags == 0x01 {
		s.tables = s.tables[:0]
	}
	for {
		data, err := s.b.ReadEvent()
		if err != nil {
			s.err = err
			return false
		}
		switch data[4] {
		case binlogFormatDescriptionEvent:
			s.headerSizes = data[75:]
		case binlogTableMapEvent:
			tm := s.parseTableMapEvent(data)
			var found bool
			if len(s.filters) > 0 {
				for _, f := range s.filters {
					if strings.HasPrefix(tm.name, f) {
						found = true
						break
					}
				}
			} else {
				found = true
			}
			if found {
				s.tables = append(s.tables, tm)
			}
		case binlogWriteRowsEventV0, binlogWriteRowsEventV1, binlogWriteRowsEventV2,
			binlogUpdateRowsEventV0, binlogUpdateRowsEventV1, binlogUpdateRowsEventV2,
			binlogDeleteRowsEventV0, binlogDeleteRowsEventV1, binlogDeleteRowsEventV2:

			s.rows = s.parseRowsEvent(data)
			if s.rows.columns == 0 {
				continue
			}
			return true
		}
	}
}

type tableMap struct {
	id       uint64
	flags    uint16
	database string
	table    string
	name     string
	types    []byte
}

// Parses MySQL's TABLE_MAP_EVENT event.
func (s *BinlogScanner) parseTableMapEvent(data []byte) (tm tableMap) {
	hsize := int(s.headerSizes[0])
	psize := int(s.headerSizes[binlogTableMapEvent])
	var idSize int
	if psize == 6 {
		idSize = 4
	} else {
		idSize = 6
	}
	var (
		idStart    = hsize
		flagStart  = idStart + idSize
		start      = hsize + psize
		dbStart    = start + 1
		dbEnd      = dbStart + int(data[start])
		tableStart = dbEnd + 2
		tableEnd   = tableStart + int(data[dbEnd+1])
		countStart = tableEnd + 1
	)
	tm.id = uint48FromBytes(data[idStart:flagStart])
	tm.flags = binary.LittleEndian.Uint16(data[flagStart : flagStart+2])
	tm.database = string(data[dbStart:dbEnd])
	tm.table = string(data[tableStart:tableEnd])
	tm.name = tm.database + "." + tm.table + "."
	count, _, n := readLengthEncodedInteger(data[countStart:])
	tm.types = data[countStart+n : countStart+n+int(count)]

	return
}

type rowsEvent struct {
	table   tableMap
	flags   uint16
	op      uint8
	columns uint64
	bitmap1 uint64
	bitmap2 uint64
	data    []byte
}

// Parses MySQL's TABLE_*_ROWS_EVENTV0-2 events.
func (s *BinlogScanner) parseRowsEvent(data []byte) (r rowsEvent) {
	evt := data[4]
	hsize := int(s.headerSizes[0])
	psize := int(s.headerSizes[evt])

	switch evt {
	case binlogWriteRowsEventV0, binlogWriteRowsEventV1, binlogWriteRowsEventV2:
		r.op = BinlogOpInsert
	case binlogUpdateRowsEventV0, binlogUpdateRowsEventV1, binlogUpdateRowsEventV2:
		r.op = BinlogOpUpdate
	case binlogDeleteRowsEventV0, binlogDeleteRowsEventV1, binlogDeleteRowsEventV2:
		r.op = BinlogOpDelete
	default:
		return
	}

	var tableId uint64
	switch psize {
	case 10:
		psize += int(binary.LittleEndian.Uint16(data[hsize+8 : hsize+10]))
		fallthrough
	case 8:
		tableId = uint48FromBytes(data[hsize : hsize+6])
		r.flags = binary.LittleEndian.Uint16(data[hsize+6 : hsize+8])
	case 6:
		tableId = uint48FromBytes(data[hsize : hsize+4])
		r.flags = binary.LittleEndian.Uint16(data[hsize+4 : hsize+6])
	default:
		panic(fmt.Sprintf("unknown post header size %d", psize))
	}

	// Find table map
	for _, tm := range s.tables {
		if tm.id == tableId {
			r.table = tm
		}
	}
	if r.table.id == 0 {
		return
	}

	// Get number of columns
	pos := hsize + psize
	var offset int
	r.columns, _, offset = readLengthEncodedInteger(data[pos:])
	if r.columns == 0 {
		return
	}
	pos += offset

	// Calculate columns-present-bitmap1
	bitmapSize := (int(r.columns) + 7) >> 3
	r.bitmap1 = uint48FromBytes(data[pos : pos+bitmapSize])
	pos += bitmapSize

	// Calculate columns-present-bitmap2 if update event
	if evt == binlogUpdateRowsEventV1 || evt == binlogUpdateRowsEventV2 {
		r.bitmap2 = uint48FromBytes(data[pos : pos+bitmapSize])
		pos += bitmapSize
	}

	// Remainder of the packet is row data
	r.data = data[pos:]
	return
}

func (s *BinlogScanner) nextRow() bool {
	if len(s.rows.data) == 0 {
		return false
	}

	row := make([]interface{}, s.rows.columns)

	if s.rows.bitmap1 > 0 {
		pos, err := s.readBinRow(row, s.rows.bitmap1)
		if err != nil {
			s.err = err
			return false
		}
		s.rows.data = s.rows.data[pos:]
	}
	if s.rows.bitmap2 > 0 {
		pos, err := s.readBinRow(row, s.rows.bitmap2)
		if err != nil {
			s.err = err
			return false
		}
		s.rows.data = s.rows.data[pos:]
	}

	s.row = row
	return true
}

func (s *BinlogScanner) readBinRow(dest []interface{}, bitmap uint64) (int, error) {
	table := s.rows.table
	data := s.rows.data
	pos := (bitCount(bitmap) + 7) >> 3
	nullBitmap := uint48FromBytes(data[0:pos])

	var err error
	var ioff int
	for i := range dest {
		// Field is not present
		if bitmap>>uint(i)&1 == 0 {
			ioff++
			continue
		}

		// Field is NULL
		if nullBitmap>>uint(i-ioff)&1 == 1 {
			continue
		}

		switch table.types[i] {
		case fieldTypeNULL:
			dest[i] = nil

		// Numeric Types
		case fieldTypeTiny:
			dest[i] = uint8(data[pos])
			pos++

		case fieldTypeShort, fieldTypeYear:
			dest[i] = uint16(binary.LittleEndian.Uint16(data[pos : pos+2]))
			pos += 2

		case fieldTypeInt24, fieldTypeLong:
			dest[i] = uint32(binary.LittleEndian.Uint32(data[pos : pos+4]))
			pos += 4

		case fieldTypeLongLong:
			dest[i] = binary.LittleEndian.Uint64(data[pos : pos+8])
			pos += 8

		case fieldTypeFloat:
			dest[i] = math.Float32frombits(binary.LittleEndian.Uint32(data[pos : pos+4]))
			pos += 4

		case fieldTypeDouble:
			dest[i] = math.Float64frombits(binary.LittleEndian.Uint64(data[pos : pos+8]))
			pos += 8

		// Length coded Binary Strings
		case fieldTypeDecimal, fieldTypeNewDecimal, fieldTypeVarChar,
			fieldTypeBit, fieldTypeEnum, fieldTypeSet, fieldTypeTinyBLOB,
			fieldTypeMediumBLOB, fieldTypeLongBLOB, fieldTypeBLOB,
			fieldTypeVarString, fieldTypeString, fieldTypeGeometry:
			var isNull bool
			var n int
			dest[i], isNull, n, err = readLengthEncodedString(data[pos:])
			pos += n
			if err != nil {
				return 0, err
			}
			if isNull {
				dest[i] = nil
			}

		// Date YYYY-MM-DD
		case fieldTypeDate, fieldTypeNewDate:
			num, isNull, n := readLengthEncodedInteger(data[pos:])
			pos += n

			if isNull {
				dest[i] = nil
				continue
			}

			dest[i], err = parseBinaryDateTime(num, data[pos:], s.loc)
			if err != nil {
				return 0, err
			}
			pos += int(num)

		// Time [-][H]HH:MM:SS[.fractal]
		case fieldTypeTime:
			num, isNull, n := readLengthEncodedInteger(data[pos:])
			pos += n

			if num == 0 {
				if isNull {
					dest[i] = nil
					continue
				} else {
					dest[i] = []byte("00:00:00")
					continue
				}
			}

			var sign string
			if data[pos] == 1 {
				sign = "-"
			}

			switch num {
			case 8:
				dest[i] = []byte(fmt.Sprintf(
					sign+"%02d:%02d:%02d",
					uint16(data[pos+1])*24+uint16(data[pos+5]),
					data[pos+6],
					data[pos+7],
				))
				pos += 8
			case 12:
				dest[i] = []byte(fmt.Sprintf(
					sign+"%02d:%02d:%02d.%06d",
					uint16(data[pos+1])*24+uint16(data[pos+5]),
					data[pos+6],
					data[pos+7],
					binary.LittleEndian.Uint32(data[pos+8:pos+12]),
				))
				pos += 12
			default:
				return 0, fmt.Errorf("Invalid TIME-packet length %d", num)
			}

		// Timestamp YYYY-MM-DD HH:MM:SS[.fractal]
		case fieldTypeTimestamp, fieldTypeDateTime:
			num, isNull, n := readLengthEncodedInteger(data[pos:])
			pos += n

			if isNull {
				dest[i] = nil
				continue
			}

			dest[i], err = parseBinaryDateTime(num, data[pos:], s.loc)
			if err != nil {
				return 0, err
			}
			pos += int(num)

		default:
			return 0, fmt.Errorf("Unknown FieldType %d", table.types[i])
		}
	}

	return pos, nil
}
