module rocksdb.env;

extern (C) {
  struct rocksdb_env_t;

  void rocksdb_env_set_background_threads(rocksdb_env_t*, int);
  void rocksdb_env_set_high_priority_background_threads(rocksdb_env_t*, int);
  void rocksdb_env_join_all_threads(rocksdb_env_t*);

  rocksdb_env_t* rocksdb_create_default_env();
  rocksdb_env_t* rocksdb_create_mem_env();
  void rocksdb_env_destroy(rocksdb_env_t*);
}

class Env {
  rocksdb_env_t* env;

  this() {
    env = rocksdb_create_default_env();
  }

  this(rocksdb_env_t* env) {
    env = env;
  }

  static Env createMemoryEnv() {
    return new Env(rocksdb_create_mem_env());
  }

  ~this() {
    rocksdb_env_destroy(env);
  }

  void joinAll() {
    rocksdb_env_join_all_threads(env);
  }

  @property backgroundThreads(int n) {
    rocksdb_env_set_background_threads(env, n);
  }

  @property highPriorityBackgroundThreads(int n) {
    rocksdb_env_set_high_priority_background_threads(env, n);
  }
}
