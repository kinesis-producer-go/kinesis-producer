// Amazon kinesis producer
// A KPL-like batch producer for Amazon Kinesis built on top of the official Go AWS SDK
// and using the same aggregation format that KPL use.
//
// Note: this project start as a fork of `tj/go-kinesis`. if you are not intersting in the
// KPL aggregation logic, you probably want to check it out.
package producer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	k "github.com/aws/aws-sdk-go-v2/service/kinesis"
	ktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/jpillora/backoff"
	"golang.org/x/sync/semaphore"
)

// Errors
var (
	ErrStoppedProducer    = errors.New("Unable to Put record. Producer is already stopped")
	ErrRecordSizeExceeded = errors.New("Data must be less than or equal to 1MB in size")
)

// Producer batches records.
type Producer struct {
	sync.RWMutex
	*Config
	aggregator *Aggregator
	semaphore  *semaphore.Weighted
	records    chan *ktypes.PutRecordsRequestEntry
	failure    chan *FailureRecord
	done       chan struct{}

	// Current state of the Producer
	// notify set to true after calling to `NotifyFailures`
	notify bool
	// stopped set to true after `Stop`ing the Producer.
	// This will prevent from user to `Put` any new data.
	stopped bool
}

// New creates new producer with the given config.
func New(config *Config) *Producer {
	config.defaults()
	return &Producer{
		Config:     config,
		done:       make(chan struct{}),
		records:    make(chan *ktypes.PutRecordsRequestEntry, config.BacklogCount),
		semaphore:  semaphore.NewWeighted(int64(config.MaxConnections)),
		aggregator: NewAggregator(),
	}
}

// Put `data` asynchronously. This method is thread-safe.
//
// Under the covers, the Producer will automatically re-attempt puts in case of
// transient errors.
// When unrecoverable error has detected(e.g: trying to put to in a stream that
// doesn't exist), the message will returned by the Producer.
// Add a listener with `Producer.NotifyFailures` to handle undeliverable messages.
func (p *Producer) Put(data []byte) error {
	p.RLock()
	stopped := p.stopped
	p.RUnlock()
	if stopped {
		return ErrStoppedProducer
	}
	if len(data) > maxRecordSize {
		return ErrRecordSizeExceeded
	}

	// if the record size is bigger than aggregation size handle it as a simple kinesis record
	if len(data) > p.AggregateBatchSize {
		p.records <- &ktypes.PutRecordsRequestEntry{
			Data:         data,
			PartitionKey: aws.String(RandPartitionKey()),
		}
		return nil
	}

	p.Lock()

	addSize := p.aggregator.CalculateAddSize(data)
	// Check if the aggregator needs to be drained using a more precise method
	// that considers the exact serialized size impact of adding the new data
	needToDrain := p.aggregator.Size()+addSize > maxRecordSize || p.aggregator.Count() >= p.AggregateBatchCount
	var (
		record *ktypes.PutRecordsRequestEntry
		err    error
	)
	if needToDrain {
		if record, err = p.aggregator.Drain(); err != nil {
			p.Logger.Error("drain aggregator", "error", err)
		}
	}
	p.aggregator.Put(data, addSize)
	p.Unlock()
	// release the lock and then pipe the record to the records channel
	// we did it, because the "send" operation blocks when the backlog is full
	// and this can cause deadlock(when we never release the lock)
	if needToDrain && record != nil {
		p.records <- record
	}

	return nil
}

// Failure record type
type FailureRecord struct {
	error
	Data         []byte
	PartitionKey string
}

// NotifyFailures registers and return listener to handle undeliverable messages.
// The incoming struct has a copy of the Data and the PartitionKey along with some
// error information about why the publishing failed.
func (p *Producer) NotifyFailures() <-chan *FailureRecord {
	p.Lock()
	defer p.Unlock()
	if !p.notify {
		p.notify = true
		p.failure = make(chan *FailureRecord, p.BacklogCount)
	}
	return p.failure
}

// Start the producer
func (p *Producer) Start() {
	p.Logger.Info("starting producer", "stream", aws.ToString(p.StreamName), "streamARN", aws.ToString(p.StreamARN))
	go p.loop()
}

// Stop the producer gracefully. Flushes any in-flight data.
func (p *Producer) Stop() {
	p.Lock()
	p.stopped = true
	p.Unlock()
	p.Logger.Info("stopping producer", "backlog", len(p.records))

	// drain
	if record, ok := p.drainIfNeed(); ok {
		p.records <- record
	}
	p.done <- struct{}{}
	close(p.records)

	// wait
	<-p.done
	// Wait for all flush goroutines to complete by acquiring all permits
	_ = p.semaphore.Acquire(context.Background(), int64(p.MaxConnections))

	// close the failures channel if we notify
	p.RLock()
	if p.notify {
		close(p.failure)
	}
	p.RUnlock()
	p.Logger.Info("stopped producer")
}

// loop and flush at the configured interval, or when the buffer is exceeded.
func (p *Producer) loop() {
	size := 0
	drain := false
	buf := make([]ktypes.PutRecordsRequestEntry, 0, p.BatchCount)
	tick := time.NewTicker(p.FlushInterval)

	flush := func(msg string) {
		_ = p.semaphore.Acquire(context.Background(), 1)
		go p.flush(buf, msg)
		buf = nil
		size = 0
	}

	bufAppend := func(record *ktypes.PutRecordsRequestEntry) {
		// the record size limit applies to the total size of the
		// partition key and data blob.
		rsize := len(record.Data) + len([]byte(*record.PartitionKey))
		if size+rsize > p.BatchSize {
			flush("batch size")
		}
		size += rsize
		buf = append(buf, *record)
		if len(buf) >= p.BatchCount {
			flush("batch length")
		}
	}

	defer tick.Stop()
	defer close(p.done)

	for {
		select {
		case record, ok := <-p.records:
			if drain && !ok {
				if size > 0 {
					flush("drain")
				}
				p.Logger.Info("backlog drained")
				return
			}
			bufAppend(record)
		case <-tick.C:
			if record, ok := p.drainIfNeed(); ok {
				bufAppend(record)
			}
			// if the buffer is still containing records
			if size > 0 {
				flush("interval")
			}
		case <-p.done:
			drain = true
		}
	}
}

func (p *Producer) drainIfNeed() (*ktypes.PutRecordsRequestEntry, bool) {
	p.RLock()
	needToDrain := p.aggregator.Count() > 0
	p.RUnlock()
	if needToDrain {
		p.Lock()
		record, err := p.aggregator.Drain()
		p.Unlock()
		if err != nil {
			p.Logger.Error("drain aggregator", "error", err)
		} else {
			return record, true
		}
	}
	return nil, false
}

// flush records and retry failures if necessary.
// for example: when we get "ProvisionedThroughputExceededException"
func (p *Producer) flush(records []ktypes.PutRecordsRequestEntry, reason string) {
	b := &backoff.Backoff{
		Jitter: true,
	}

	defer p.semaphore.Release(1)

	for {
		p.Logger.Info("flushing records", "reason", reason, "records", len(records))
		out, err := p.Client.PutRecords(context.Background(), &k.PutRecordsInput{
			StreamARN:  p.StreamARN,
			StreamName: p.StreamName,
			Records:    records,
		})

		if err != nil {
			p.Logger.Error("flush", "error", err)
			p.RLock()
			notify := p.notify
			p.RUnlock()
			if notify {
				p.dispatchFailures(records, err)
			}
			return
		}

		if p.Verbose {
			for i, r := range out.Records {
				if r.ErrorCode != nil {
					p.Logger.Info(fmt.Sprintf("Result[%d]", i), "ErrorCode", aws.ToString(r.ErrorCode), "ErrorMessage", aws.ToString(r.ErrorMessage))
				} else {
					p.Logger.Info(fmt.Sprintf("Result[%d]", i), "ShardId", aws.ToString(r.ShardId), "SequenceNumber", aws.ToString(r.SequenceNumber))
				}
			}
		}

		failed := *out.FailedRecordCount
		if failed == 0 {
			return
		}

		duration := b.Duration()

		p.Logger.Info("put failures", "failures", failed, "backoff", duration.String())
		time.Sleep(duration)

		// change the logging state for the next itertion
		reason = "retry"
		records = failures(records, out.Records)
	}
}

// dispatchFailures gets batch of records, extract them, and push them
// into the failure channel
func (p *Producer) dispatchFailures(records []ktypes.PutRecordsRequestEntry, err error) {
	for _, r := range records {
		if isAggregated(&r) {
			p.dispatchFailures(extractRecords(&r), err)
		} else {
			p.failure <- &FailureRecord{err, r.Data, *r.PartitionKey}
		}
	}
}

// failures returns the failed records as indicated in the response.
func failures(records []ktypes.PutRecordsRequestEntry,
	response []ktypes.PutRecordsResultEntry) (out []ktypes.PutRecordsRequestEntry) {
	for i, record := range response {
		if record.ErrorCode != nil {
			out = append(out, records[i])
		}
	}
	return
}
