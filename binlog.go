package mysql

const (
	BinlogBlock uint16 = iota
	BinlogNonBlock
)

const (
	binlogUnknownEvent byte = iota
	binlogStartEventV3
	binlogQueryEvent
	binlogStopEvent
	binlogRotateEvent
	binlogIntvarEvent
	binlogLoadEvent
	binlogSlaveEvent
	binlogCreateFileEvent
	binlogAppendBlockEvent
	binlogExecLoadEvent
	binlogDeleteFileEvent
	binlogNewLoadEvent
	binlogRandEvent
	binlogUserVarEvent
	binlogFormatDescriptionEvent
	binlogXidEvent
	binlogBeginLoadQueryEvent
	binlogExecuteLoadQueryEvent
	binlogTableMapEvent
	binlogWriteRowsEventV0
	binlogUpdateRowsEventV0
	binlogDeleteRowsEventV0
	binlogWriteRowsEventV1
	binlogUpdateRowsEventV1
	binlogDeleteRowsEventV1
	binlogIncidentEvent
	binlogHeartbeatEvent
	binlogIgnorableEvent
	binlogRowsQueryEvent
	binlogWriteRowsEventV2
	binlogUpdateRowsEventV2
	binlogDeleteRowsEventV2
)

type Binlog interface {
	ReadEvent() ([]byte, error)
	Close() error
}
