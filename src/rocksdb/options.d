module rocksdb.options;

import std.conv : to;
import std.string : fromStringz;
import core.stdc.stdlib : cfree = free;

import rocksdb.comparator;
import rocksdb.env : Env, rocksdb_env_t;
import rocksdb.snapshot : Snapshot, rocksdb_snapshot_t;

extern (C) {
	struct rocksdb_options_t;
	struct rocksdb_writeoptions_t;
	struct rocksdb_readoptions_t;

	rocksdb_options_t* rocksdb_options_create();
	void rocksdb_options_destroy(rocksdb_options_t*);
	void rocksdb_options_increase_parallelism(rocksdb_options_t*, int total_threads);
	void rocksdb_options_set_create_if_missing(rocksdb_options_t*, ubyte);
	void rocksdb_options_set_create_missing_column_families(rocksdb_options_t*, ubyte);
	void rocksdb_options_set_error_if_exists(rocksdb_options_t*, ubyte);
	void rocksdb_options_set_paranoid_checks(rocksdb_options_t*, ubyte);
	void rocksdb_options_set_env(rocksdb_options_t*, rocksdb_env_t*);
	void rocksdb_options_set_compression(rocksdb_options_t*, int);
	void rocksdb_options_set_compaction_style(rocksdb_options_t*, int);
	void rocksdb_options_set_comparator(rocksdb_options_t*, rocksdb_comparator_t*);
	void rocksdb_options_enable_statistics(rocksdb_options_t*);
	char* rocksdb_options_statistics_get_string(rocksdb_options_t*);

	rocksdb_writeoptions_t* rocksdb_writeoptions_create();
	void rocksdb_writeoptions_destroy(rocksdb_writeoptions_t*);
	void rocksdb_writeoptions_set_sync(rocksdb_writeoptions_t*, ubyte);
	void rocksdb_writeoptions_disable_WAL(rocksdb_writeoptions_t*, int);

	rocksdb_readoptions_t* rocksdb_readoptions_create();
	void rocksdb_readoptions_destroy(rocksdb_readoptions_t*);
	void rocksdb_readoptions_set_verify_checksums(rocksdb_readoptions_t*, ubyte);
	void rocksdb_readoptions_set_fill_cache(rocksdb_readoptions_t*, ubyte);
	void rocksdb_readoptions_set_snapshot(rocksdb_readoptions_t*, const rocksdb_snapshot_t*);
	// void rocksdb_readoptions_set_iterate_upper_bound(rocksdb_readoptions_t*, const char*, size_t);
	void rocksdb_readoptions_set_read_tier(rocksdb_readoptions_t*, int);
	void rocksdb_readoptions_set_tailing(rocksdb_readoptions_t*, ubyte);
	void rocksdb_readoptions_set_readahead_size(rocksdb_readoptions_t*, size_t);
}

enum CompressionType {
	NONE = 0x0,
	SNAPPY = 0x1,
	ZLIB = 0x2,
	BZIP2 = 0x3,
	LZ4 = 0x4,
	LZ4HC = 0x5,
	XPRESS = 0x6,
	ZSTD = 0x7,
}

enum CompactionStyle {
	LEVEL = 0,
	UNIVERSAL = 1,
	FIFO = 2,
}

enum ReadTier {
	READ_ALL = 0x0,
	BLOCK_CACHE = 0x1,
	PERSISTED = 0x2,
}

class WriteOptions {
	rocksdb_writeoptions_t* opts;

	this() {
		opts = rocksdb_writeoptions_create();
	}

	~this() {
		rocksdb_writeoptions_destroy(opts);
	}

@property:
	void sync(bool v) {
		rocksdb_writeoptions_set_sync(opts, cast(ubyte)v);
	}

	void disableWAL(bool v) {
		rocksdb_writeoptions_disable_WAL(opts, cast(int)v);
	}
}

class ReadOptions {
	rocksdb_readoptions_t* opts;

	this() {
		opts = rocksdb_readoptions_create();
	}

	~this() {
		rocksdb_readoptions_destroy(opts);
	}

@property:
	void verifyChecksums(bool v) {
		rocksdb_readoptions_set_verify_checksums(opts, v);
	}

	void fillCache(bool v) {
		rocksdb_readoptions_set_fill_cache(opts, v);
	}

	void readTier(ReadTier tier) {
		rocksdb_readoptions_set_read_tier(opts, tier);
	}

	void tailing(bool v) {
		rocksdb_readoptions_set_tailing(opts, v);
	}

	void readAheadSize(size_t size) {
		rocksdb_readoptions_set_readahead_size(opts, size);
	}

	void snapshot(Snapshot snap) {
		rocksdb_readoptions_set_snapshot(opts, snap.snap);
	}
}

class DBOptions {
	rocksdb_options_t* opts;

	this() {
		opts = rocksdb_options_create();
	}

	~this() {
		rocksdb_options_destroy(opts);
	}

	void enableStatistics() {
		rocksdb_options_enable_statistics(opts);
	}

	string getStatisticsString() {
		char* cresult = rocksdb_options_statistics_get_string(opts);
		string result = fromStringz(cresult).to!string;
		cfree(cresult);
		return result;
	}

@property:
	void parallelism(int totalThreads) {
		rocksdb_options_increase_parallelism(opts, totalThreads);
	}

	void createIfMissing(bool value) {
		rocksdb_options_set_create_if_missing(opts, cast(ubyte)value);
	}

	void createMissingColumnFamilies(bool value) {
		rocksdb_options_set_create_missing_column_families(opts, cast(ubyte)value);
	}

	void errorIfExists(bool value) {
		rocksdb_options_set_error_if_exists(opts, cast(ubyte)value);
	}

	void paranoidChecks(bool value) {
		rocksdb_options_set_paranoid_checks(opts, cast(ubyte)value);
	}

	void env(Env env) {
		rocksdb_options_set_env(opts, env.env);
	}

	void compression(CompressionType type) {
		rocksdb_options_set_compression(opts, type);
	}

	void compactionStyle(CompactionStyle style) {
		rocksdb_options_set_compaction_style(opts, style);
	}

	void comparator(Comparator cmp) {
		rocksdb_options_set_comparator(opts, cmp.cmp);
	}
}
