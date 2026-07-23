package producer

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"sync"
	"testing"
	"time"

	k "github.com/aws/aws-sdk-go-v2/service/kinesis"
	ktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

// recordingMock captures every PutRecords call (its records, deep enough to read
// Data and PartitionKey) and lets a test drive per-call responses. It is safe for
// the concurrent flush goroutines to call.
type recordingMock struct {
	mu    sync.Mutex
	calls [][]ktypes.PutRecordsRequestEntry
	// respFn returns the response for call number `call`; nil means "always succeed".
	respFn func(call int, records []ktypes.PutRecordsRequestEntry) (*k.PutRecordsOutput, error)
}

func (m *recordingMock) PutRecords(_ context.Context, in *k.PutRecordsInput, _ ...func(*k.Options)) (*k.PutRecordsOutput, error) {
	m.mu.Lock()
	call := len(m.calls)
	recs := make([]ktypes.PutRecordsRequestEntry, len(in.Records))
	copy(recs, in.Records)
	m.calls = append(m.calls, recs)
	fn := m.respFn
	m.mu.Unlock()
	if fn != nil {
		return fn(call, recs)
	}
	return &k.PutRecordsOutput{FailedRecordCount: new(int32(0))}, nil
}

func (m *recordingMock) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

func (m *recordingMock) snapshot() [][]ktypes.PutRecordsRequestEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([][]ktypes.PutRecordsRequestEntry, len(m.calls))
	copy(out, m.calls)
	return out
}

// deliveredPayloads flattens every call, deaggregating aggregated entries, into the
// list of user payloads that actually reached the wire.
func deliveredPayloads(calls [][]ktypes.PutRecordsRequestEntry) []string {
	var out []string
	for _, batch := range calls {
		for i := range batch {
			e := batch[i]
			if isAggregated(&e) {
				for _, sub := range extractRecords(&e) {
					out = append(out, string(sub.Data))
				}
			} else {
				out = append(out, string(e.Data))
			}
		}
	}
	return out
}

func multiset(ss []string) map[string]int {
	m := make(map[string]int, len(ss))
	for _, s := range ss {
		m[s]++
	}
	return m
}

func assertSameMultiset(t *testing.T, got, want []string) {
	t.Helper()
	g, w := multiset(got), multiset(want)
	if len(g) != len(w) {
		t.Errorf("delivered %d distinct payloads, want %d\n\tgot:  %v\n\twant: %v", len(g), len(w), got, want)
		return
	}
	for k, wc := range w {
		if g[k] != wc {
			t.Errorf("payload %q delivered %d times, want %d", k, g[k], wc)
		}
	}
}

// TestFlushDrainsAggregatorOnStop verifies that records still buffered in the
// aggregator at Stop time (below every count/size/interval threshold) are drained,
// aggregated, and flushed exactly once — the "flush in-flight data on Stop" contract.
func TestFlushDrainsAggregatorOnStop(t *testing.T) {
	m := &recordingMock{}
	p := New(&Config{
		StreamName:     new("foo"),
		MaxConnections: 1,
		FlushInterval:  time.Hour, // never let the ticker fire
		Client:         m,
	})
	p.Start()
	payloads := []string{"one", "two", "three"}
	for _, s := range payloads {
		if err := p.Put([]byte(s)); err != nil {
			t.Fatal(err)
		}
	}
	// nothing should have flushed yet: below AggregateBatchCount/Size, ticker disabled
	if c := m.callCount(); c != 0 {
		t.Fatalf("flushed %d times before Stop, want 0 (data should sit in aggregator)", c)
	}
	p.Stop()

	calls := m.snapshot()
	if len(calls) != 1 {
		t.Fatalf("Stop produced %d PutRecords calls, want 1", len(calls))
	}
	if len(calls[0]) != 1 {
		t.Fatalf("drain flushed %d entries, want 1 aggregated entry", len(calls[0]))
	}
	if !isAggregated(&calls[0][0]) {
		t.Errorf("Stop drain did not produce an aggregated record")
	}
	assertSameMultiset(t, deliveredPayloads(calls), payloads)
}

// TestPutDrainsAggregatorBySize covers the AggregateBatchSize splitting contract:
// with a small AggregateBatchSize the buffered records are split into multiple
// aggregated records (rather than one oversized one), and all are delivered.
func TestPutDrainsAggregatorBySize(t *testing.T) {
	m := &recordingMock{}
	p := New(&Config{
		StreamName:         new("foo"),
		MaxConnections:     1,
		AggregateBatchSize: 200,       // force a drain every ~12 tiny records
		FlushInterval:      time.Hour, // isolate size as the only split cause
		Client:             m,
	})
	p.Start()
	var want []string
	for i := range 50 {
		s := "rec-" + itoa4(i)
		want = append(want, s)
		if err := p.Put([]byte(s)); err != nil {
			t.Fatal(err)
		}
	}
	p.Stop()

	calls := m.snapshot()
	if len(calls) != 1 {
		t.Fatalf("got %d PutRecords calls, want 1 (BatchCount/BatchSize huge)", len(calls))
	}
	if len(calls[0]) < 2 {
		t.Fatalf("got %d aggregated entries, want >1 (size threshold should split)", len(calls[0]))
	}
	for i := range calls[0] {
		if !isAggregated(&calls[0][i]) {
			t.Errorf("entry %d is not an aggregated record", i)
		}
	}
	assertSameMultiset(t, deliveredPayloads(calls), want)
}

// TestFlushByBatchSize covers the loop's byte-threshold flush (size+rsize >
// BatchSize) in bufAppend. Records bypass aggregation so each entry has a known
// wire size: 112B data ("payload-" + 4-digit index + 100B) + 8B partition key =
// 120B. With a 250B budget the loop fits at most 2 records per request, so 5
// records take 3 requests. We assert the PUBLIC BatchSize contract (no request
// exceeds BatchSize) plus the request count, not a specific greedy partition.
func TestFlushByBatchSize(t *testing.T) {
	m := &recordingMock{}
	const batchSize = 250
	p := New(&Config{
		StreamName:         new("foo"),
		MaxConnections:     1,
		AggregateBatchSize: 20,        // records below are aggregated; ours are 112B -> bypass
		BatchSize:          batchSize, // 120B/record on the wire => at most 2 per request
		FlushInterval:      time.Hour, // isolate BatchSize as the only split cause
		Client:             m,
	})
	p.Start()
	var want []string
	for i := range 5 {
		s := "payload-" + itoa4(i) + string(make([]byte, 100)) // 112B data, >20B => bypass
		want = append(want, s)
		if err := p.Put([]byte(s)); err != nil {
			t.Fatal(err)
		}
	}
	p.Stop()

	calls := m.snapshot()
	if len(calls) != 3 {
		t.Fatalf("got %d PutRecords calls, want 3 (250B budget, 120B/record)", len(calls))
	}
	for i, batch := range calls {
		if ws := callWireSize(batch); ws > batchSize {
			t.Errorf("request %d wire size %d exceeds BatchSize %d", i, ws, batchSize)
		}
		for j := range batch {
			if isAggregated(&batch[j]) {
				t.Errorf("record should have bypassed aggregation: %q", batch[j].Data)
			}
		}
	}
	assertSameMultiset(t, deliveredPayloads(calls), want)
}

// TestFlushByInterval covers the ticker branch (<-tick.C -> drainIfNeed -> flush):
// records sitting in the aggregator are flushed by the interval alone, before Stop.
func TestFlushByInterval(t *testing.T) {
	m := &recordingMock{}
	p := New(&Config{
		StreamName:     new("foo"),
		MaxConnections: 1,
		FlushInterval:  20 * time.Millisecond, // the only reason these records flush
		Client:         m,
	})
	p.Start()
	want := []string{"a", "b"}
	for _, s := range want {
		if err := p.Put([]byte(s)); err != nil {
			t.Fatal(err)
		}
	}
	// wait for the interval ticker to flush, without Stop
	deadline := time.Now().Add(2 * time.Second)
	for m.callCount() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("interval never flushed buffered records within 2s")
		}
		time.Sleep(2 * time.Millisecond)
	}
	p.Stop()
	assertSameMultiset(t, deliveredPayloads(m.snapshot()), want)
}

// TestRetryPreservesFailedRecordIdentity verifies that on a partial (per-record)
// failure the producer retries exactly the failed record, not merely the right
// count. Records bypass aggregation so Data identifies each one on the wire.
func TestRetryPreservesFailedRecordIdentity(t *testing.T) {
	a := "AAAAA"
	b := "BBBBB"
	c := "CCCCC"
	m := &recordingMock{
		respFn: func(call int, _ []ktypes.PutRecordsRequestEntry) (*k.PutRecordsOutput, error) {
			if call == 0 {
				// only the middle record (b) fails
				return &k.PutRecordsOutput{
					FailedRecordCount: new(int32(1)),
					Records: []ktypes.PutRecordsResultEntry{
						{SequenceNumber: new("s0"), ShardId: new("sh")},
						{ErrorCode: new("ProvisionedThroughputExceededException"), ErrorMessage: new("throttle")},
						{SequenceNumber: new("s2"), ShardId: new("sh")},
					},
				}, nil
			}
			return &k.PutRecordsOutput{FailedRecordCount: new(int32(0))}, nil
		},
	}
	p := New(&Config{
		StreamName:         new("foo"),
		MaxConnections:     1,
		BatchCount:         3,
		AggregateBatchSize: 1,         // all records bypass aggregation
		FlushInterval:      time.Hour, // batch flushes by count(3), not interval
		Client:             m,
	})
	p.Start()
	for _, s := range []string{a, b, c} {
		if err := p.Put([]byte(s)); err != nil {
			t.Fatal(err)
		}
	}
	p.Stop()

	calls := m.snapshot()
	if len(calls) != 2 {
		t.Fatalf("got %d PutRecords calls, want 2 (initial + 1 retry)", len(calls))
	}
	if len(calls[0]) != 3 {
		t.Fatalf("initial call carried %d records, want 3", len(calls[0]))
	}
	if len(calls[1]) != 1 {
		t.Fatalf("retry carried %d records, want 1", len(calls[1]))
	}
	if got := string(calls[1][0].Data); got != b {
		t.Errorf("retry sent %q, want the failed record %q", got, b)
	}
}

// TestPutBypassesAggregationForLargeRecord covers the branch in Put where a record
// larger than AggregateBatchSize is sent as a plain (non-aggregated) record.
func TestPutBypassesAggregationForLargeRecord(t *testing.T) {
	m := &recordingMock{}
	p := New(&Config{
		StreamName:         new("foo"),
		MaxConnections:     1,
		AggregateBatchSize: 50,
		FlushInterval:      time.Hour,
		Client:             m,
	})
	p.Start()
	big := make([]byte, 100) // > AggregateBatchSize
	for i := range big {
		big[i] = 'z'
	}
	if err := p.Put(big); err != nil {
		t.Fatal(err)
	}
	p.Stop()

	calls := m.snapshot()
	if len(calls) != 1 || len(calls[0]) != 1 {
		t.Fatalf("got calls %v, want a single request with a single record", callShape(calls))
	}
	if isAggregated(&calls[0][0]) {
		t.Errorf("large record was aggregated, want a plain record")
	}
	if string(calls[0][0].Data) != string(big) {
		t.Errorf("delivered data mismatch for bypassed record")
	}
}

// TestPutAfterStopReturnsError covers Put's stopped guard.
func TestPutAfterStopReturnsError(t *testing.T) {
	m := &recordingMock{}
	p := New(&Config{
		StreamName:     new("foo"),
		MaxConnections: 1,
		Client:         m,
	})
	p.Start()
	p.Stop()
	if err := p.Put([]byte("late")); err != ErrStoppedProducer {
		t.Errorf("Put after Stop returned %v, want ErrStoppedProducer", err)
	}
}

// TestPutRecordSizeExceeded covers Put's size guard.
func TestPutRecordSizeExceeded(t *testing.T) {
	m := &recordingMock{}
	p := New(&Config{
		StreamName:     new("foo"),
		MaxConnections: 1,
		Client:         m,
	})
	if err := p.Put(make([]byte, maxRecordSize+1)); err != ErrRecordSizeExceeded {
		t.Errorf("oversized Put returned %v, want ErrRecordSizeExceeded", err)
	}
}

// itoa4 renders n as a 4-digit zero-padded string (deterministic distinct labels
// without pulling in fmt for hot test loops).
func itoa4(n int) string {
	b := []byte{'0', '0', '0', '0'}
	for i := 3; i >= 0 && n > 0; i-- {
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b)
}

func callShape(calls [][]ktypes.PutRecordsRequestEntry) []int {
	out := make([]int, len(calls))
	for i, c := range calls {
		out[i] = len(c)
	}
	return out
}

// callWireSize sums the on-the-wire size (data + partition key) of a request's
// records, matching how loop() accounts BatchSize.
func callWireSize(batch []ktypes.PutRecordsRequestEntry) int {
	n := 0
	for i := range batch {
		n += len(batch[i].Data) + len(*batch[i].PartitionKey)
	}
	return n
}

// aggregatedPayloads deaggregates a single aggregated entry into its user payloads.
func aggregatedPayloads(t *testing.T, e ktypes.PutRecordsRequestEntry) []string {
	t.Helper()
	if !isAggregated(&e) {
		t.Fatalf("entry is not an aggregated record")
	}
	var out []string
	for _, sub := range extractRecords(&e) {
		out = append(out, string(sub.Data))
	}
	return out
}

// TestDispatchFailuresPlainRecord covers dispatchFailures' plain (non-aggregated)
// record path: a record larger than AggregateBatchSize bypasses aggregation, and on
// a transport error its original Data/PartitionKey are surfaced verbatim.
func TestDispatchFailuresPlainRecord(t *testing.T) {
	m := &recordingMock{
		respFn: func(int, []ktypes.PutRecordsRequestEntry) (*k.PutRecordsOutput, error) {
			return nil, errors.New("transport failure")
		},
	}
	p := New(&Config{
		StreamName:         new("foo"),
		MaxConnections:     1,
		BatchCount:         1,
		AggregateBatchSize: 50,
		FlushInterval:      time.Hour,
		Client:             m,
	})
	failures := p.NotifyFailures()
	var got []FailureRecord
	done := make(chan struct{})
	go func() {
		for f := range failures {
			got = append(got, *f)
		}
		close(done)
	}()
	p.Start()
	big := bytes.Repeat([]byte("X"), 100) // > AggregateBatchSize => bypass aggregation
	if err := p.Put(big); err != nil {
		t.Fatal(err)
	}
	p.Stop()
	<-done

	if len(got) != 1 {
		t.Fatalf("got %d failure records, want 1", len(got))
	}
	if string(got[0].Data) != string(big) {
		t.Errorf("failure Data = %q..., want the original bypassed record", got[0].Data[:min(8, len(got[0].Data))])
	}
	if got[0].PartitionKey == "" {
		t.Errorf("failure PartitionKey is empty")
	}
	if got[0].error == nil {
		t.Errorf("failure error is nil")
	}
}

// TestFlushErrorNoListenerDropsSilently covers flush's notify==false branch: a
// transport error with no NotifyFailures listener drops the records silently, and
// Stop must still complete (no panic, no semaphore deadlock).
func TestFlushErrorNoListenerDropsSilently(t *testing.T) {
	m := &recordingMock{
		respFn: func(int, []ktypes.PutRecordsRequestEntry) (*k.PutRecordsOutput, error) {
			return nil, errors.New("transport failure")
		},
	}
	p := New(&Config{
		StreamName:     new("foo"),
		MaxConnections: 1,
		BatchCount:     1,
		FlushInterval:  time.Hour,
		Client:         m,
	})
	p.Start()
	if err := p.Put([]byte("dropme")); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { p.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop deadlocked when flush failed with no failure listener")
	}
	if c := m.callCount(); c != 1 {
		t.Errorf("PutRecords called %d times, want 1 (no retry on transport error)", c)
	}
}

// TestDispatchFailuresAggregatedFallback covers dispatchFailures' fallback: an entry
// that passes isAggregated (magic + payload + valid MD5) but whose payload is not a
// decodable AggregatedRecord yields no extracted records, so the whole entry must be
// dispatched as a single failure rather than being dropped.
func TestDispatchFailuresAggregatedFallback(t *testing.T) {
	m := &recordingMock{
		respFn: func(int, []ktypes.PutRecordsRequestEntry) (*k.PutRecordsOutput, error) {
			return nil, errors.New("transport failure")
		},
	}
	p := New(&Config{
		StreamName:         new("foo"),
		MaxConnections:     1,
		BatchCount:         1,
		AggregateBatchSize: 10,
		FlushInterval:      time.Hour,
		Client:             m,
	})
	failures := p.NotifyFailures()
	var got []FailureRecord
	done := make(chan struct{})
	go func() {
		for f := range failures {
			got = append(got, *f)
		}
		close(done)
	}()
	p.Start()
	// magic + undecodable payload + matching MD5: isAggregated true, extract empty.
	payload := bytes.Repeat([]byte{0xFF}, 40)
	sum := md5.Sum(payload)
	entry := append(append(append([]byte{}, magicNumber...), payload...), sum[:]...)
	if !isAggregated(&ktypes.PutRecordsRequestEntry{Data: entry}) {
		t.Fatal("test setup: crafted entry is not recognized as aggregated")
	}
	if err := p.Put(entry); err != nil { // len 60 > AggregateBatchSize => sent verbatim
		t.Fatal(err)
	}
	p.Stop()
	<-done

	if len(got) != 1 {
		t.Fatalf("got %d failure records, want 1 (whole-entry fallback)", len(got))
	}
	if !bytes.Equal(got[0].Data, entry) {
		t.Errorf("fallback failure Data = %d bytes, want the whole %d-byte entry", len(got[0].Data), len(entry))
	}
}

// TestRetryPreservesAggregatedFailedRecord covers retry identity for the common
// production shape: a batch of multiple aggregated entries where one fails. The
// retry must carry the correct aggregated entry, verified by deaggregating it.
func TestRetryPreservesAggregatedFailedRecord(t *testing.T) {
	m := &recordingMock{
		respFn: func(call int, _ []ktypes.PutRecordsRequestEntry) (*k.PutRecordsOutput, error) {
			if call == 0 {
				// two aggregated entries in the batch; only the second fails
				return &k.PutRecordsOutput{
					FailedRecordCount: new(int32(1)),
					Records: []ktypes.PutRecordsResultEntry{
						{SequenceNumber: new("s0"), ShardId: new("sh")},
						{ErrorCode: new("ProvisionedThroughputExceededException")},
					},
				}, nil
			}
			return &k.PutRecordsOutput{FailedRecordCount: new(int32(0))}, nil
		},
	}
	p := New(&Config{
		StreamName:          new("foo"),
		MaxConnections:      1,
		AggregateBatchCount: 2,         // drain every 2 records => 2 aggregates from 4
		FlushInterval:       time.Hour, // batch flushes once, at Stop
		Client:              m,
	})
	p.Start()
	for _, s := range []string{"g0", "g1", "g2", "g3"} {
		if err := p.Put([]byte(s)); err != nil {
			t.Fatal(err)
		}
	}
	p.Stop()

	calls := m.snapshot()
	if len(calls) != 2 {
		t.Fatalf("got %d PutRecords calls, want 2 (initial + retry)", len(calls))
	}
	if len(calls[0]) != 2 {
		t.Fatalf("initial call carried %d aggregated entries, want 2", len(calls[0]))
	}
	if len(calls[1]) != 1 {
		t.Fatalf("retry carried %d entries, want 1", len(calls[1]))
	}
	assertSameMultiset(t, aggregatedPayloads(t, calls[1][0]), []string{"g2", "g3"})
}
