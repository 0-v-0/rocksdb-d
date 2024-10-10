module rocksdb.iterator;

import std.conv : to;
import std.string : fromStringz, toStringz;

import rocksdb.options : ReadOptions, rocksdb_readoptions_t;
import rocksdb.database : Database, rocksdb_t;
import rocksdb.columnfamily : ColumnFamily, rocksdb_column_family_handle_t;

private extern (C) {
	struct rocksdb_iterator_t;

	rocksdb_iterator_t* rocksdb_create_iterator(rocksdb_t*, rocksdb_readoptions_t*);
	rocksdb_iterator_t* rocksdb_create_iterator_cf(rocksdb_t*, rocksdb_readoptions_t*, rocksdb_column_family_handle_t*);

	void rocksdb_iter_destroy(rocksdb_iterator_t*);
	ubyte rocksdb_iter_valid(const rocksdb_iterator_t*);
	void rocksdb_iter_seek_to_first(rocksdb_iterator_t*);
	void rocksdb_iter_seek_to_last(rocksdb_iterator_t*);
	void rocksdb_iter_seek(rocksdb_iterator_t*, const char*, size_t);
	void rocksdb_iter_seek_for_prev(rocksdb_iterator_t*, const char*, size_t);
	void rocksdb_iter_next(rocksdb_iterator_t*);
	void rocksdb_iter_prev(rocksdb_iterator_t*);
	immutable(char*) rocksdb_iter_key(const rocksdb_iterator_t*, size_t*);
	immutable(char*) rocksdb_iter_value(const rocksdb_iterator_t*, size_t*);
	void rocksdb_iter_get_error(const rocksdb_iterator_t*, char**);
}

class Iterator {
	rocksdb_iterator_t* iter;

	this(Database db, ReadOptions opts) {
		iter = rocksdb_create_iterator(db.db, opts.opts);
		seekToFirst();
	}

	this(Database db, ColumnFamily family, ReadOptions opts) {
		iter = rocksdb_create_iterator_cf(db.db, opts.opts, family.cf);
		seekToFirst();
	}

	~this() {
		rocksdb_iter_destroy(iter);
	}

	void seekToFirst() {
		rocksdb_iter_seek_to_first(iter);
	}

	void seekToLast() {
		rocksdb_iter_seek_to_last(iter);
	}

	void seek(string key) {
		seek(cast(ubyte[])key);
	}

	void seek(in ubyte[] key) {
		rocksdb_iter_seek(iter, cast(char*)key.ptr, key.length);
	}

	void seekPrev(string key) {
		seekPrev(cast(ubyte[])key);
	}

	void seekPrev(in ubyte[] key) {
		rocksdb_iter_seek_for_prev(iter, cast(char*)key.ptr, key.length);
	}

	void next() {
		rocksdb_iter_next(iter);
	}

	void prev() {
		rocksdb_iter_prev(iter);
	}

	bool valid() {
		return cast(bool)rocksdb_iter_valid(iter);
	}

	ubyte[] key() {
		size_t size;
		immutable char* ckey = rocksdb_iter_key(iter, &size);
		return cast(ubyte[])ckey[0..size];
	}

	ubyte[] value() {
		size_t size;
		immutable char* cvalue = rocksdb_iter_value(iter, &size);
		return cast(ubyte[])cvalue[0..size];
	}

	int opApply(scope int delegate(ubyte[], ubyte[]) dg) {
		int result = 0;

		while (valid()) {
			result = dg(key(), value());
			if (result) break;
			next();
		}

		return result;
	}

	void close() {
		destroy(this);
	}
}
