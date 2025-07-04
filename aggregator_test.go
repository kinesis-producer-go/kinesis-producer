package producer

import (
	"math/rand"
	"strconv"
	"sync"
	"testing"
)

func assert(t *testing.T, val bool, msg string) {
	if !val {
		t.Error(msg)
	}
}

func TestSizeAndCount(t *testing.T) {
	a := NewAggregator()
	assert(t, a.Count() == 0, "size and count should equal to 0 at the beginning")
	assert(t, a.Size() == a.calculateInitialSize(), "size should equal to initial size at the beginning")

	for i := 0; i < 2000; i++ {
		data := []byte("hello")
		n := rand.Intn(1000) + 1
		for j := 0; j < n; j++ {
			addSize := a.CalculateAddSize(data)
			a.Put(data, addSize)
		}

		calCount := a.Count()
		calSize := a.Size()
		entry, _ := a.Drain()

		assert(t, calCount == n, "count should be equal to the number of Put calls")
		assert(t, calSize == len(entry.Data), "size should equal to the serialized data")
	}
}

func TestAggregation(t *testing.T) {
	var wg sync.WaitGroup
	a := NewAggregator()
	n := 50
	wg.Add(n)
	for i := 0; i < n; i++ {
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
	assert(t, isAggregated(record), "should return an agregated record")
	records := extractRecords(record)
	for i := 0; i < n; i++ {
		c := strconv.Itoa(i)
		found := false
		for _, record := range records {
			if string(record.Data) == "hello-"+c {
				assert(t, string(record.Data) == "hello-"+c, "`Data` field contains invalid value")
				found = true
			}
		}
		assert(t, found, "record not found after extracting: "+c)
	}
}

func TestDrainEmptyAggregator(t *testing.T) {
	a := NewAggregator()
	entry, err := a.Drain()
	assert(t, entry == nil, "should return an nil entry")
	assert(t, err == nil, "should not return an error")
}
