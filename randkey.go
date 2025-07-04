package producer

import (
	"encoding/binary"
	"encoding/hex"
	"math/rand/v2"
)

// RandPartitionKey generates a random 8-character hexadecimal string.
// The string contains characters from 0-f (hexadecimal digits).
// It creates a 4-byte random value and converts it to hex encoding,
// resulting in an 8-character string since each byte becomes 2 hex characters.
func RandPartitionKey() string {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, rand.Uint32())
	str := hex.EncodeToString(b)
	return str
}
