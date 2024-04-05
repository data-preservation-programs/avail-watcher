package db

type NetworkType uint32

const (
	UNKNOWN NetworkType = iota
	AVAIL_GOLDBERG_TESTNET
)

func (n *NetworkType) FromString(s string) {
	switch s {
	case "avail-goldberg-testnet":
		*n = AVAIL_GOLDBERG_TESTNET
	default:
		panic("unknown network type")
	}
}

type Block struct {
	Hash    string      `gorm:"primarykey"`
	Network NetworkType `gorm:"index:idx_network_height"`
	Height  uint64      `gorm:"index:idx_network_height"`
	Cid     string
}

type Manifest struct {
	Cid    string `gorm:"primarykey"`
	S3Url  string
	Offset uint64
	Length uint64
}

type Piece struct {
	Network   NetworkType
	PieceCid  string
	PieceSize uint64
	Url       string
	Size      uint64
	RootCid   string
}
