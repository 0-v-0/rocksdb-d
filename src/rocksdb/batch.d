module rocksdb.batch;

import std.string : toStringz;

import rocksdb.options,
rocksdb.queryable,
rocksdb.columnfamily;

extern (C) {
	struct rocksdb_writebatch_t;

	rocksdb_writebatch_t* rocksdb_writebatch_create();
	rocksdb_writebatch_t* rocksdb_writebatch_create_from(const char*, size_t);
	void rocksdb_writebatch_destroy(rocksdb_writebatch_t*);
	void rocksdb_writebatch_clear(rocksdb_writebatch_t*);
	int rocksdb_writebatch_count(rocksdb_writebatch_t*);

	void rocksdb_writebatch_put(rocksdb_writebatch_t*, const char*, size_t, const char*, size_t);
	void rocksdb_writebatch_put_cf(rocksdb_writebatch_t*, rocksdb_column_family_handle_t*, const char*, size_t, const char*, size_t);

	void rocksdb_writebatch_delete(rocksdb_writebatch_t*, const char*, size_t);
	void rocksdb_writebatch_delete_cf(rocksdb_writebatch_t*, rocksdb_column_family_handle_t*, const char*, size_t);
}

class WriteBatch {
	mixin Putable;
	mixin Removeable;

	rocksdb_writebatch_t* batch;

	this() {
		batch = rocksdb_writebatch_create();
	}

	this(string frm) {
		batch = rocksdb_writebatch_create_from(toStringz(frm), frm.length);
	}

	~this() {
		rocksdb_writebatch_destroy(batch);
	}

	void clear() {
		rocksdb_writebatch_clear(batch);
	}

	int count() {
		return rocksdb_writebatch_count(batch);
	}

	void putImpl(in void[] key, in void[] value, ColumnFamily family, WriteOptions opts = null)
	in (opts is null, "WriteBatch cannot use WriteOptions") {

		if (family) {
			rocksdb_writebatch_put_cf(
				batch,
				family.cf,
				cast(char*)key.ptr,
				key.length,
				cast(char*)value.ptr,
				value.length);
		} else {
			rocksdb_writebatch_put(
				batch,
				cast(char*)key.ptr,
				key.length,
				cast(char*)value.ptr,
				value.length);
		}
	}

	void removeImpl(in void[] key, ColumnFamily family, WriteOptions opts = null)
	in (opts is null, "WriteBatch cannot use WriteOptions") {
		if (family) {
			rocksdb_writebatch_delete_cf(
				batch,
				family.cf,
				cast(char*)key.ptr,
				key.length);
		} else {
			rocksdb_writebatch_delete(
				batch,
				cast(char*)key.ptr,
				key.length);
		}
	}
}
