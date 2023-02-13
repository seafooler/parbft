package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
)

func genMsgHashSum(data []byte) ([]byte, error) {
	msgHash := sha256.New()
	_, err := msgHash.Write(data)
	if err != nil {
		return nil, err
	}
	return msgHash.Sum(nil), nil
}

// encode encodes the data into bytes.
// Data can be of any type.
// Examples can be seen form the tests.
func encode(data interface{}) ([]byte, error) {
	buf := bytes.Buffer{}
	enc := json.NewEncoder(&buf)
	if err := enc.Encode(data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// decode decodes bytes into the data.
// Data should be passed in the format of a pointer to a type.
// Examples can be seen form the tests.
func decode(s []byte, data interface{}) error {
	dec := json.NewDecoder(bytes.NewReader(s))
	if err := dec.Decode(data); err != nil {
		return err
	}
	return nil
}
