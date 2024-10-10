module rocksdb.queryable;

import std.conv;

import rocksdb.options;

mixin template Getable() {
	public import std.conv : to;
	public import rocksdb.options : ReadOptions, WriteOptions;

	/// Get a key
	ubyte[] get(in void[] key, ReadOptions opts = null) {
		return getImpl(key, null, opts);
	}
}

mixin template Putable() {
	void put(in void[] key, in void[] value, WriteOptions opts = null) {
		return putImpl(key, value, null, opts);
	}
}

mixin template Removeable() {
	void remove(in void[] key, WriteOptions opts = null) {
		removeImpl(key, null, opts);
	}
}
