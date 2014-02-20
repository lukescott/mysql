package mysql

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strconv"
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

// Reads arbitrary length integer into uint64.
func uint48FromBytes(b []byte) (n uint64) {
	var s uint8
	for _, c := range b {
		n |= uint64(c) << s
		s += 8
	}
	return
}

// Counts number of on vs off bits.
func bitCount(x uint64) (n int) {
	x -= ((x >> 1) & 0x5555555555555555)
	x = (x & 0x3333333333333333) + ((x >> 2) & 0x3333333333333333)
	n = int((((x + (x >> 4)) & 0x0F0F0F0F0F0F0F0F) * 0x0101010101010101) >> 56)
	return
}

var errNilPtr = errors.New("destination pointer is nil")

// convertAssign copies to dest the value in src, converting it if possible.
// An error is returned if the copy would result in loss of information.
// Uses dest to determine signed-ness of src due to MySQL Bug #71687.
// dest should be a pointer type.
func convertAssign(dest interface{}, src interface{}) error {
	switch s := src.(type) {
	case string:
		switch d := dest.(type) {
		case *string:
			if d == nil {
				return errNilPtr
			}
			*d = s
		case *[]byte:
			if d == nil {
				return errNilPtr
			}
			*d = []byte(s)
		case *interface{}:
			if d == nil {
				return errNilPtr
			}
			*d = s
		default:
			goto Reflect
		}
	case []byte:
		switch d := dest.(type) {
		case *string:
			if d == nil {
				return errNilPtr
			}
			*d = string(s)
		case *[]byte:
			if d == nil {
				return errNilPtr
			}
			*d = s
		case *interface{}:
			if d == nil {
				return errNilPtr
			}
			*d = s
		default:
			goto Reflect
		}
	case uint8, uint16, uint32, uint64:
		var i64 int64

		// Determine signed-ness from dest
		switch dest.(type) {
		case *uint, *uint8, *uint16, *uint32, *uint64:
			switch n := src.(type) {
			case uint8:
				i64 = int64(n)
			case uint16:
				i64 = int64(n)
			case uint32:
				i64 = int64(n)
			case uint64:
				i64 = int64(n)
			}
		default:
			switch n := src.(type) {
			case uint8:
				i64 = int64(int8(n))
			case uint16:
				i64 = int64(int16(n))
			case uint32:
				i64 = int64(int32(n))
			case uint64:
				i64 = int64(n)
			}
		}

		switch d := dest.(type) {
		case *int:
			if d == nil {
				return errNilPtr
			}
			*d = int(i64)
		case *int8:
			if d == nil {
				return errNilPtr
			}
			*d = int8(i64)
		case *int16:
			if d == nil {
				return errNilPtr
			}
			*d = int16(i64)
		case *int32:
			if d == nil {
				return errNilPtr
			}
			*d = int32(i64)
		case *int64:
			if d == nil {
				return errNilPtr
			}
			*d = int64(i64)
		case *uint:
			if d == nil {
				return errNilPtr
			}
			*d = uint(i64)
		case *uint8:
			if d == nil {
				return errNilPtr
			}
			*d = uint8(i64)
		case *uint16:
			if d == nil {
				return errNilPtr
			}
			*d = uint16(i64)
		case *uint32:
			if d == nil {
				return errNilPtr
			}
			*d = uint32(i64)
		case *uint64:
			if d == nil {
				return errNilPtr
			}
			*d = uint64(i64)
		case *float32:
			if d == nil {
				return errNilPtr
			}
			*d = float32(i64)
		case *float64:
			if d == nil {
				return errNilPtr
			}
			*d = float64(i64)
		case *string:
			if d == nil {
				return errNilPtr
			}
			*d = strconv.FormatInt(i64, 10)
		case *[]byte:
			if d == nil {
				return errNilPtr
			}
			*d = []byte(strconv.FormatInt(i64, 10))
		case *interface{}:
			if d == nil {
				return errNilPtr
			}
			*d = i64
		default:
			goto Reflect
		}
	case float32:
		switch d := dest.(type) {
		case *float32:
			if d == nil {
				return errNilPtr
			}
			*d = s
		case *float64:
			if d == nil {
				return errNilPtr
			}
			*d = float64(s)
		case *string:
			if d == nil {
				return errNilPtr
			}
			*d = strconv.FormatFloat(float64(s), 'e', -1, 32)
		case *[]byte:
			if d == nil {
				return errNilPtr
			}
			*d = []byte(strconv.FormatFloat(float64(s), 'e', -1, 32))
		case *interface{}:
			if d == nil {
				return errNilPtr
			}
			*d = s
		default:
			goto Reflect
		}
	case float64:
		switch d := dest.(type) {
		case *float32:
			if d == nil {
				return errNilPtr
			}
			*d = float32(s)
		case *float64:
			if d == nil {
				return errNilPtr
			}
			*d = s
		case *string:
			if d == nil {
				return errNilPtr
			}
			*d = strconv.FormatFloat(s, 'e', -1, 64)
		case *[]byte:
			if d == nil {
				return errNilPtr
			}
			*d = []byte(strconv.FormatFloat(s, 'e', -1, 64))
		case *interface{}:
			if d == nil {
				return errNilPtr
			}
			*d = s
		default:
			goto Reflect
		}
	case nil:
		switch d := dest.(type) {
		case *interface{}:
			if d == nil {
				return errNilPtr
			}
			*d = nil
		case *[]byte:
			if d == nil {
				return errNilPtr
			}
			*d = nil
		default:
			goto Reflect
		}
	}
	return nil

Reflect:
	if scanner, is := dest.(sql.Scanner); is {
		return scanner.Scan(src)
	}

	dpv := reflect.ValueOf(dest)
	if dpv.Kind() != reflect.Ptr {
		return errors.New("destination not a pointer")
	}
	if dpv.IsNil() {
		return errNilPtr
	}

	dv := reflect.Indirect(dpv)
	if src == nil {
		dv.Set(reflect.Zero(dv.Type()))
		return nil
	}

	switch dv.Kind() {
	case reflect.String:
		switch s := src.(type) {
		case uint8:
			dv.SetString(strconv.FormatInt(int64(int8(s)), 10))
		case uint16:
			dv.SetString(strconv.FormatInt(int64(int16(s)), 10))
		case uint32:
			dv.SetString(strconv.FormatInt(int64(int32(s)), 10))
		case uint64:
			dv.SetString(strconv.FormatInt(int64(s), 10))
		case float32:
			dv.SetString(strconv.FormatFloat(float64(s), 'e', -1, 32))
		case float64:
			dv.SetString(strconv.FormatFloat(s, 'e', -1, 64))
		case string:
			dv.SetString(s)
		case []byte:
			dv.SetString(string(s))
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch s := src.(type) {
		case uint8:
			dv.SetInt(int64(int8(s)))
		case uint16:
			dv.SetInt(int64(int16(s)))
		case uint32:
			dv.SetInt(int64(int32(s)))
		case uint64:
			dv.SetInt(int64(s))
		case string:
			i64, err := strconv.ParseInt(s, 10, dv.Type().Bits())
			if err != nil {
				return fmt.Errorf("converting string %q to a %s: %v", s, dv.Kind(), err)
			}
			dv.SetInt(i64)
		default:
			goto Failed
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		switch s := src.(type) {
		case uint8:
			dv.SetUint(uint64(s))
		case uint16:
			dv.SetUint(uint64(s))
		case uint32:
			dv.SetUint(uint64(s))
		case uint64:
			dv.SetUint(uint64(s))
		case string:
			u64, err := strconv.ParseUint(s, 10, dv.Type().Bits())
			if err != nil {
				return fmt.Errorf("converting string %q to a %s: %v", s, dv.Kind(), err)
			}
			dv.SetUint(u64)
		default:
			goto Failed
		}
	case reflect.Float32, reflect.Float64:
		switch s := src.(type) {
		case float32, float64:
			dv.SetFloat(reflect.ValueOf(s).Float())
		case uint8:
			dv.SetFloat(float64(int8(s)))
		case uint16:
			dv.SetFloat(float64(int16(s)))
		case uint32:
			dv.SetFloat(float64(int32(s)))
		case uint64:
			dv.SetFloat(float64(int64(s)))
		case string:
			f64, err := strconv.ParseFloat(s, dv.Type().Bits())
			if err != nil {
				return fmt.Errorf("converting string %q to a %s: %v", s, dv.Kind(), err)
			}
			dv.SetFloat(f64)
		}
	case reflect.Ptr:
		dv.Set(reflect.New(dv.Type().Elem()))
		return convertAssign(dv.Interface(), src)
	default:
		sv := reflect.ValueOf(src)
		if dv.Kind() != sv.Kind() {
			goto Failed
		}
		dv.Set(sv)
	}

	return nil

Failed:
	return fmt.Errorf("unsupported driver -> Scan pair: %T -> %T", src, dest)
}
