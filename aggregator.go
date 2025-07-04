package producer

import (
	"bytes"
	"crypto/md5"

	"github.com/aws/aws-sdk-go-v2/aws"
	ktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

var (
	magicNumber = []byte{0xF3, 0x89, 0x9A, 0xC2}
)

type Aggregator struct {
	buf    []*Record
	nbytes int
}

// calculateInitialSize computes the initial size of an empty aggregated record.
// This includes the magic number, MD5 checksum, and the partition key table overhead.
func (a *Aggregator) calculateInitialSize() int {
	initialSize := len(magicNumber) + md5.Size

	// Tag-Length-Value size for string partition_key_table = 1 in AggregatedRecord;
	initialSize += protowire.SizeTag(1)
	initialSize += protowire.SizeVarint(8)
	initialSize += 8

	return initialSize
}

// NewAggregator creates a new aggregator with proper initialization
func NewAggregator() *Aggregator {
	a := &Aggregator{
		buf: make([]*Record, 0),
	}
	a.nbytes = a.calculateInitialSize()
	return a
}

// Size return how many bytes if all records in the aggregator stored serialized to KPL Aggregated Record format.
// Including the magic number, protobuf message and checksum.
func (a *Aggregator) Size() int {
	return a.nbytes
}

// Count return how many records stored in the aggregator.
func (a *Aggregator) Count() int {
	return len(a.buf)
}

// CalculateAddSize calculates the byte size increment that would be added to the final
// serialized aggregated record if the given data is added. This includes the protobuf
// wire format overhead for the record and the aggregated record structure.
// This method does not modify the aggregator state.
func (a *Aggregator) CalculateAddSize(data []byte) int {
	// Record wire size
	recordSize := 0
	// Tag-Value size for required uint64 partition_key_index = 1 in message Record;
	recordSize += protowire.SizeTag(1)
	recordSize += protowire.SizeVarint(uint64(0))
	// Tag-Length-Value size for required bytes data = 3 in message Record;
	recordSize += protowire.SizeTag(3)
	recordSize += protowire.SizeVarint(uint64(len(data)))
	recordSize += len(data)

	// AggregatedRecord add wire size
	addSize := 0
	// Tag-Length-Value size for repeated Record records = 3 in message AggregatedRecord;
	addSize += protowire.SizeTag(3)
	addSize += protowire.SizeVarint(uint64(recordSize))
	addSize += recordSize

	return addSize
}

// Put record using `data`. This method is thread-safe.
func (a *Aggregator) Put(data []byte, addSize int) {
	zero := uint64(0)
	a.buf = append(a.buf, &Record{
		Data:              data,
		PartitionKeyIndex: &zero,
	})

	a.nbytes += addSize
}

// Drain create an aggregated `kinesis.PutRecordsRequestEntry`
// that compatible with the KCL's deaggregation logic.
//
// If you interested to know more about it. see: aggregation-format.md
func (a *Aggregator) Drain() (*ktypes.PutRecordsRequestEntry, error) {
	if a.Count() == 0 {
		return nil, nil
	}

	partitionKey := RandPartitionKey()
	aggregatedRecordData, err := proto.Marshal(&AggregatedRecord{
		PartitionKeyTable: []string{partitionKey},
		Records:           a.buf,
	})
	if err != nil {
		return nil, err
	}
	h := md5.New()
	h.Write(aggregatedRecordData)
	checkSum := h.Sum(nil)

	var buffer bytes.Buffer
	buffer.Write(magicNumber)
	buffer.Write(aggregatedRecordData)
	buffer.Write(checkSum)

	entry := &ktypes.PutRecordsRequestEntry{
		Data:         buffer.Bytes(),
		PartitionKey: aws.String(partitionKey),
	}
	a.clear()
	return entry, nil
}

func (a *Aggregator) clear() {
	a.buf = make([]*Record, 0)
	a.nbytes = a.calculateInitialSize()
}

// Test if a given entry is aggregated record.
func isAggregated(entry *ktypes.PutRecordsRequestEntry) bool {
	return bytes.HasPrefix(entry.Data, magicNumber)
}

func extractRecords(entry *ktypes.PutRecordsRequestEntry) (out []ktypes.PutRecordsRequestEntry) {
	src := entry.Data[len(magicNumber) : len(entry.Data)-md5.Size]
	dest := new(AggregatedRecord)
	err := proto.Unmarshal(src, dest)
	if err != nil {
		return
	}
	for i := range dest.Records {
		r := dest.Records[i]
		out = append(out, ktypes.PutRecordsRequestEntry{
			Data:         r.GetData(),
			PartitionKey: &dest.PartitionKeyTable[r.GetPartitionKeyIndex()],
		})
	}
	return
}
