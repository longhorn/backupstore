package backupstore

type ProgressState string

const (
	ProgressStateStarting   = ProgressState("starting")
	ProgressStateInProgress = ProgressState("in_progress")
	ProgressStateComplete   = ProgressState("complete")
	ProgressStateError      = ProgressState("error")
)

type Mapping struct {
	Offset int64
	Size   int64
}

type Mappings struct {
	Mappings  []Mapping
	BlockSize int64
}

type MessageType string

const (
	MessageTypeError = MessageType("error")
)
