package util

import (
	"github.com/cockroachdb/errors"
	commcid "github.com/filecoin-project/go-fil-commcid"
	commp "github.com/filecoin-project/go-fil-commp-hashhash"
	"github.com/ipfs/go-cid"
)

func GetCommp(calc *commp.Calc, targetPieceSize uint64) (cid.Cid, uint64, error) {
	rawCommp, rawPieceSize, err := calc.Digest()
	if err != nil {
		return cid.Undef, 0, errors.WithStack(err)
	}

	if rawPieceSize < targetPieceSize {
		rawCommp, err = commp.PadCommP(rawCommp, rawPieceSize, targetPieceSize)
		if err != nil {
			return cid.Undef, 0, errors.WithStack(err)
		}

		rawPieceSize = targetPieceSize
	}

	commCid, err := commcid.DataCommitmentV1ToCID(rawCommp)
	if err != nil {
		return cid.Undef, 0, errors.WithStack(err)
	}

	return commCid, rawPieceSize, nil
}
