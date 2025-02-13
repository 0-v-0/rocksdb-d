module rocksdb.snapshot;

import rocksdb.database : Database, rocksdb_t;

package extern (C) {
	struct rocksdb_snapshot_t;

	rocksdb_snapshot_t* rocksdb_create_snapshot(rocksdb_t*);
	void rocksdb_release_snapshot(rocksdb_t*, const rocksdb_snapshot_t*);
}

class Snapshot {
	Database db;
	rocksdb_snapshot_t* snap;

	this(Database db) {
		snap = rocksdb_create_snapshot(db.db);
	}

	~this() {
		rocksdb_release_snapshot(db.db, snap);
	}
}