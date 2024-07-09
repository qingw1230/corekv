package codec

import "encoding/json"

func WalCodec(entry *Entry) []byte {
	data, _ := json.Marshal(entry)
	return data
}

func ValuePtrCodec(ptr *ValuePtr) []byte {
	return []byte{}
}
