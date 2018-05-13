package aggregator

// A PartitionedAggregator works like an Aggregator, but every resulting
// Kinesis record contains only records for a single partitionkey.   This
// increases the number of records and lowers the efficiency of record packing
// for minimizing PUT values, but if your consumers care about partitioning of
// records between shards, this is is necessary.
//
// Not implemented yet.
type PartitionedAggregator struct{}
