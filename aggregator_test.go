package producer

import (
	"bytes"
	"crypto/md5"
	"math/rand"
	"strconv"
	"sync"
	"testing"

	ktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/go-openapi/testify/v2/assert"
	"github.com/kinesis-producer-go/kinesis-producer/internal/kpl"
	"google.golang.org/protobuf/proto"
)

func TestSizeAndCount(t *testing.T) {
	a := newAggregator()
	assert.Equal(t, a.Count(), 0, "size and count should equal to 0 at the beginning")
	assert.Equal(t, a.Size(), a.calculateInitialSize(), "size should equal to initial size at the beginning")

	for range 2000 {
		data := []byte("hello")
		n := rand.Intn(1000) + 1
		for range n {
			addSize := a.CalculateAddSize(data)
			a.Put(data, addSize)
		}

		calCount := a.Count()
		calSize := a.Size()
		entry, _ := a.Drain()

		assert.Equal(t, calCount, n, "count should be equal to the number of Put calls")
		assert.Equal(t, calSize, len(entry.Data), "size should equal to the serialized data")
	}
}

func TestAggregation(t *testing.T) {
	var wg sync.WaitGroup
	a := newAggregator()
	n := 50
	wg.Add(n)
	for i := range n {
		c := strconv.Itoa(i)
		data := []byte("hello-" + c)
		addSize := a.CalculateAddSize(data)
		a.Put(data, addSize)
		wg.Done()
	}
	wg.Wait()
	record, err := a.Drain()
	if err != nil {
		t.Error(err)
	}
	assert.True(t, isAggregated(record), "should return an agregated record")
	records := extractRecords(record)
	for i := range n {
		c := strconv.Itoa(i)
		found := false
		for _, record := range records {
			if string(record.Data) == "hello-"+c {
				assert.Equal(t, string(record.Data), "hello-"+c, "`Data` field contains invalid value")
				found = true
			}
		}
		assert.True(t, found, "record not found after extracting: "+c)
	}
}

func TestIsAggregated(t *testing.T) {
	a := newAggregator()
	data := []byte("hello")
	a.Put(data, a.CalculateAddSize(data))
	entry, err := a.Drain()
	assert.Nil(t, err)
	assert.True(t, isAggregated(entry), "real aggregate should pass all checks")

	short := &ktypes.PutRecordsRequestEntry{Data: []byte{0xF3, 0x89, 0x9A, 0xC2, 'h', 'i'}}
	assert.False(t, isAggregated(short), "magic prefix without room for the checksum")

	fake := &ktypes.PutRecordsRequestEntry{Data: append([]byte{0xF3, 0x89, 0x9A, 0xC2}, make([]byte, 40)...)}
	assert.False(t, isAggregated(fake), "magic prefix with an invalid checksum")

	entry.Data[5] ^= 0xFF
	assert.False(t, isAggregated(entry), "corrupted aggregate must fail the checksum")
}

func TestDrainEmptyAggregator(t *testing.T) {
	a := newAggregator()
	entry, err := a.Drain()
	assert.Nil(t, entry, "should return an nil entry")
	assert.Nil(t, err, "should not return an error")
}

// TestDrainMatchesReferenceEncoding guards the envelope Drain assembles by hand.
func TestDrainMatchesReferenceEncoding(t *testing.T) {
	for _, size := range []int{0, 1, 127, 128, 1024, 16384} {
		name := "size " + strconv.Itoa(size) + ": "
		a := newAggregator()
		records := make([][]byte, 5)
		for i := range records {
			records[i] = bytes.Repeat([]byte{byte(i + 1)}, size)
			a.Put(records[i], a.CalculateAddSize(records[i]))
		}
		want := a.Size()

		entry, err := a.Drain()
		assert.Nil(t, err, name+"drain should not fail")
		assert.Equal(t, len(entry.Data), want, name+"entry length should match the size the aggregator promised")

		protos := make([]*kpl.Record, len(records))
		for i, data := range records {
			protos[i] = kpl.Record_builder{PartitionKeyIndex: proto.Uint64(0), Data: data}.Build()
		}
		message, err := proto.Marshal(kpl.AggregatedRecord_builder{
			PartitionKeyTable: []string{*entry.PartitionKey},
			Records:           protos,
		}.Build())
		assert.Nil(t, err, name+"reference marshal should not fail")
		checkSum := md5.Sum(message)
		// Literal, so that editing the constant cannot make this check agree with it.
		reference := append([]byte{0xF3, 0x89, 0x9A, 0xC2}, message...)
		reference = append(reference, checkSum[:]...)

		assert.True(t, bytes.Equal(entry.Data, reference), name+"entry should match the reference encoding byte for byte")
	}
}
