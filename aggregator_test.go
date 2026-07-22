package producer

import (
	"math/rand"
	"strconv"
	"sync"
	"testing"

	ktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/go-openapi/testify/v2/assert"
)

func TestSizeAndCount(t *testing.T) {
	a := NewAggregator()
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
	a := NewAggregator()
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
	a := NewAggregator()
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
	a := NewAggregator()
	entry, err := a.Drain()
	assert.Nil(t, entry, "should return an nil entry")
	assert.Nil(t, err, "should not return an error")
}
