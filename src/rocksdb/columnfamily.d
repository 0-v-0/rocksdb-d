module rocksdb.columnfamily;

import rocksdb.options : rocksdb_options_t, ReadOptions, WriteOptions;
import rocksdb.iterator : Iterator;
import rocksdb.database : Database, rocksdb_t, ensureRocks;
import rocksdb.queryable : Getable, Putable, Removeable;

extern (C) {
	struct rocksdb_column_family_handle_t;

	char** rocksdb_list_column_families(const rocksdb_options_t*, const char*, size_t*, char**);
	void rocksdb_list_column_families_destroy(char**, size_t);

	rocksdb_column_family_handle_t* rocksdb_create_column_family(rocksdb_t*, const rocksdb_options_t*, const char*, char**);
	void rocksdb_drop_column_family(rocksdb_t*, rocksdb_column_family_handle_t*, char**);
	void rocksdb_column_family_handle_destroy(rocksdb_column_family_handle_t*);
}

class ColumnFamily {
	mixin Getable;
	mixin Putable;
	mixin Removeable;

	Database db;
	string name;
	rocksdb_column_family_handle_t* cf;

	this(Database db, string name, rocksdb_column_family_handle_t* cf) {
		db = db;
		name = name;
		cf = cf;
	}

	Iterator iter(ReadOptions opts = null) {
		return new Iterator(db, this, opts ? opts : db.readOptions);
	}

	void withIter(void delegate(Iterator) dg, ReadOptions opts = null) {
		Iterator iter = iter(opts);
		scope (exit) destroy(iter);
		dg(iter);
	}

	void drop() {
		char* err = null;
		rocksdb_drop_column_family(db.db, cf, &err);
		err.ensureRocks();
	}

	ubyte[][] multiGet(ubyte[][] keys, ReadOptions opts = null) {
		return db.multiGet(keys, this, opts);
	}

	string[] multiGetString(string[] keys, ReadOptions opts = null) {
		return db.multiGetString(keys, this, opts);
	}

	ubyte[] getImpl(in void[] key, ColumnFamily family, ReadOptions opts = null) {
		assert(family == this || family is null);
		return db.getImpl(key, this, opts);
	}

	void putImpl(in void[] key, in void[] value, ColumnFamily family, WriteOptions opts = null) {
		assert(family == this || family is null);
		db.putImpl(key, value, this, opts);
	}

	void removeImpl(in void[] key, ColumnFamily family, WriteOptions opts = null) {
		assert(family == this || family is null);
		db.removeImpl(key, this, opts);
	}
}

unittest {
	import std.stdio : writeln;
	import std.conv : to;
	import std.algorithm.searching : startsWith;
	import rocksdb.options : DBOptions, CompressionType;

	writeln("Testing Column Families");

	// DB Options
	auto opts = new DBOptions;
	opts.createIfMissing = true;
	opts.errorIfExists = false;
	opts.compression = CompressionType.NONE;

	// Create the database (if it does not exist)
	auto db = new Database(opts, "test");

	string[] columnFamilies = [
		"test",
		"test1",
		"test2",
		"test3",
		"test4",
		"wow",
	];

	// create a bunch of column families
	foreach (cf; columnFamilies) {
		if (cf !in db.columnFamilies) {
			db.createColumnFamily(cf);
		}
	}

	db.close();
	db = new Database(opts, "test");
	scope (exit) destroy(db);

	// Test column family listing
	assert(Database.listColumnFamilies(opts, "test").length == columnFamilies.length + 1);

	void testColumnFamily(ColumnFamily cf, int times) {
		for (int i = 0; i < times; i++) {
			cf.put(cf.name ~ i.to!string, i.to!string);
		}

		for (int i = 0; i < times; i++) {
			assert(cf.get(cf.name ~ i.to!string) == i.to!string);
		}

		cf.withIter((iter) {
			foreach (key, value; iter) {
				assert(key.startsWith(cf.name));
			}
		});
	}

	foreach (name, cf; db.columnFamilies) {
		if (name == "default") continue;

		writeln("  ", name);
		testColumnFamily(cf, 1000);
	}
}
