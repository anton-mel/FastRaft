package labgob

//
// trying to send non-capitalized fields over RPC produces a range of
// misbehavior, including both mysterious incorrect computation and
// outright crashes. so this wrapper around Go's encoding/gob warns
// about non-capitalized field names.
//

import (
	"encoding/gob"
	"io"
	"reflect"
)

var errorCount int // for TestCapital

type LabEncoder struct {
	gob *gob.Encoder
}

func NewEncoder(w io.Writer) *LabEncoder {
	enc := &LabEncoder{}
	enc.gob = gob.NewEncoder(w)
	return enc
}

func (enc *LabEncoder) Encode(e interface{}) error {
	return enc.gob.Encode(e)
}

func (enc *LabEncoder) EncodeValue(value reflect.Value) error {
	return enc.gob.EncodeValue(value)
}

type LabDecoder struct {
	gob *gob.Decoder
}

func NewDecoder(r io.Reader) *LabDecoder {
	dec := &LabDecoder{}
	dec.gob = gob.NewDecoder(r)
	return dec
}

func (dec *LabDecoder) Decode(e interface{}) error {
	return dec.gob.Decode(e)
}

func Register(value interface{}) {
	gob.Register(value)
}

func RegisterName(name string, value interface{}) {
	gob.RegisterName(name, value)
}
