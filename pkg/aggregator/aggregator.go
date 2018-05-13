package aggregator

import (
	"bytes"
	"crypto/md5"
	"errors"
	"fmt"
	"hash/fnv"

	k "github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/golang/protobuf/proto"
	"github.com/kimutansk/go-kinesis-deaggregation/pb"
)

// This is the magic header prefix defined by the KPL/KCL libraries.
var magicNumber = []byte{0xF3, 0x89, 0x9A, 0xC2}

// A Partitioner returns the Kinesis partition key from the given record.
type Partitioner interface {
	Partition([]byte) string
}

type bodyHashPartitioner struct{}

func (b bodyHashPartitioner) Partition(body []byte) string {
	h := fnv.New64a()
	h.Write(body)
	v := h.Sum64()

	return fmt.Sprintf("%x", v)
}

func finalizeAggregated(records []*pb.Record, partitions map[string]int) (*k.PutRecordsRequestEntry, error) {
	if len(records) == 0 || len(partitions) == 0 {
		return nil, errors.New("no records to finalize")
	}
	keys := make([]string, len(partitions))
	for k, v := range partitions {
		keys[v] = k
	}

	raw, err := proto.Marshal(&pb.AggregatedRecord{
		PartitionKeyTable: keys,
		Records:           records,
	})
	if err != nil {
		return nil, err
	}

	data := bytes.Buffer{}
	data.Write(magicNumber)
	data.Write(raw)

	h := md5.New()
	h.Write(raw)
	data.Write(h.Sum(nil))

	batchKey := keys[0]

	entry := &k.PutRecordsRequestEntry{
		Data:         data.Bytes(),
		PartitionKey: &batchKey,
	}
	return entry, nil
}

// An Aggregator accepts []byte records, using the protobuf format of the
// KPL/KCL to concatenate multiple small records into more efficient larger
// ones.
//
// If a Kinesis Record contains multiple aggregated user records, the resulting
// Kinesis PutRecordRequestEntry will be dispatched to the partition of the
// first user record in the aggregate.
type Aggregator struct {
	// TargetSize indicates the desired size for aggregating smaller records
	// together.  If not given, 25kb (1 PUT Record) is default.
	TargetSize int
	// Partitioner is used if Put() is called without a key.  By default this
	// will be a hash of the record bytes.
	KeyPartitioner Partitioner

	initialized   bool
	partIDs       map[string]int
	records       []*pb.Record
	curSize       int
	nbyte         int
	nrec          int
	completedRecs []*k.PutRecordsRequestEntry
}

func (a *Aggregator) initialize() {
	if a.TargetSize == 0 {
		a.TargetSize = 25000 // 1 PUT record unit.
	}
	if a.KeyPartitioner == nil {
		a.KeyPartitioner = bodyHashPartitioner{}
	}
	if a.records == nil {
		a.records = []*pb.Record{}
	}
	if a.partIDs == nil {
		a.partIDs = map[string]int{}
	}
	if a.completedRecs == nil {
		a.completedRecs = []*k.PutRecordsRequestEntry{}
	}
}

// Put enters a new client record into the Aggregator.  If key is given, it
// will be used as the Kinesis partition key.  If no key is provided, or the
// key is not valid, one will be calculated based on the Aggregator's
// configured partitioner (which is a hash of the record bytes by default).
//
// Put returns the record offset slot that the AggregatedRecord will appear in
// when Drain() is called (and thus the slot it would be in if the output of
// Drain is passed directly to kinesis.PutRecords()).  This can be used
// to map source records to Kinesis records in the event that PutRecord fails
// an individual PutRecordsRequestEntry.
func (a *Aggregator) Put(b []byte, key ...string) (int, error) {
	if !a.initialized {
		a.initialize()
	}

	var partkey string
	if len(key) > 0 {
		partkey = key[0]
	}
	if partkey == "" || len(partkey) > 255 {
		partkey = a.KeyPartitioner.Partition(b)
	}

	recsize := len(b)
	if recsize > a.TargetSize {
		// Don't aggregate this one.  Big enough to be its own record.
		entry := &k.PutRecordsRequestEntry{
			Data:         b,
			PartitionKey: &partkey,
		}
		a.completedRecs = append(a.completedRecs, entry)
		a.nbyte += (recsize + len(partkey))
		a.nrec++
		return len(a.completedRecs), nil
	}

	// If this record would put us over target size and we've already go records,
	// the turn them into a single AggregatedRecord and add the new record as
	// the first in the next batch.
	if len(a.records) > 0 && a.curSize+recsize+len(partkey) > a.TargetSize {
		recs, err := finalizeAggregated(a.records, a.partIDs)
		if err != nil {
			return -1, err
		}

		a.completedRecs = append(a.completedRecs, recs)

		a.records = a.records[:0]
		a.curSize = 0
		a.partIDs = map[string]int{}
	}

	partIdx := uint64(0)
	idx, ok := a.partIDs[partkey]
	if !ok {
		npart := len(a.partIDs)
		a.partIDs[partkey] = npart
		partIdx = uint64(npart)
		recsize += len(partkey)
	} else {
		partIdx = uint64(idx)
	}

	a.records = append(a.records, &pb.Record{
		Data:              b,
		PartitionKeyIndex: &partIdx,
	})
	a.curSize += recsize
	a.nbyte += recsize
	a.nrec++

	return len(a.completedRecs), nil
}

// Size is the total byte size of the Aggregator, including data records and
// partition keys.
func (a *Aggregator) Size() int {
	return a.nbyte
}

// Count is the number of user records in the Aggregator, before aggregation.
func (a *Aggregator) Count() int {
	return a.nrec
}

// Recs is the total number of Kinesis Data Stream records, after per-record
// aggregation.
func (a *Aggregator) Recs() int {
	if !a.initialized {
		a.initialize()
	}
	inflight := 0
	if len(a.records) > 0 {
		inflight++
	}
	return len(a.completedRecs) + inflight
}

// Drain returns all records currently in the Aggregator in the form of
// kinesis.PutRecordRequestEntry, suitable for input to kinesis.PutRecords.
func (a *Aggregator) Drain() ([]*k.PutRecordsRequestEntry, error) {
	if !a.initialized {
		a.initialize()
	}
	ret := a.completedRecs
	if len(a.records) > 0 {
		recs, err := finalizeAggregated(a.records, a.partIDs)
		if err != nil {
			return nil, err
		}
		ret = append(ret, recs)
	}

	a.nrec = 0
	a.nbyte = 0
	a.curSize = 0
	a.partIDs = map[string]int{}
	a.records = a.records[:0]
	a.completedRecs = a.completedRecs[:0]

	return ret, nil
}
