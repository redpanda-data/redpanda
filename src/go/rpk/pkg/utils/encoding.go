package utils

import (
	bytes2 "bytes"
	"encoding/binary"
)

func WriteString(bs []byte, str string) ([]byte, error) {
	buf := new(bytes2.Buffer)
	stringBytes := []byte(str)
	length := int32(len(stringBytes))
	err := binary.Write(buf, binary.LittleEndian, length)
	if err != nil {
		return nil, err
	}
	_, err = buf.Write(stringBytes)
	if err != nil {
		return nil, err
	}
	return append(bs, buf.Bytes()...), nil
}

func WriteBuffer(bs []byte, bs2 []byte) ([]byte, error) {
	buf := new(bytes2.Buffer)
	length := int32(len(bs2))
	err := binary.Write(buf, binary.LittleEndian, length)
	if err != nil {
		return nil, err
	}
	_, err = buf.Write(bs2)
	if err != nil {
		return nil, err
	}
	return append(bs, buf.Bytes()...), nil
}
