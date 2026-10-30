package core

import (
	"crypto/rand"
	"encoding/binary"
	mathRand "math/rand/v2"

	"github.com/codeyu/cronv5/internal/mtx"
)

var RandSrc mtx.Mtx[*mathRand.Rand]

func init() {
	var seedBytes [16]byte
	_, _ = rand.Read(seedBytes[:])
	seedState := binary.LittleEndian.Uint64(seedBytes[:8])
	seedStream := binary.LittleEndian.Uint64(seedBytes[8:])
	RandSrc.Set(mathRand.New(mathRand.NewPCG(seedState, seedStream)))
}
