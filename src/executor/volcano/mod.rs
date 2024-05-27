use txn_storage::prelude::*;

enum OpIterator {
    Scan(DatabaseId, ContainerId, ScanOptions),
    Project,
    Filter,
    HashJoin,
    Sort,
    Aggregate,
}
