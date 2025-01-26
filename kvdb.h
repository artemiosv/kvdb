#ifndef KVDB_H
#define KVDB_H

#include "sqlite3.h"

#define KVDB_RETURN_OK 0
#define KVDB_RETURN_ERROR 1

#define KVDB_DB_FILENAME "kvdb.db"
#define KVDB_TS_CHAR_LENGTH 24 /* Fits format YYYY-MM-DD HH:MM:SS.SSS */

typedef struct {
	sqlite3 *db;
} KVDB;

void usage(const char *exec_name);
int kvdb_open_db(KVDB *kvdb, const char *filename);
int kvdb_close_db(KVDB *kvdb);
int kvdb_create_table_in_db(KVDB *kvdb);

int kvdb_set_key(KVDB *kvdb, const char *key, const char *value);
const char *kvdb_get_key_value(KVDB *kvdb, const char *key);
int kvdb_del_key(KVDB *kvdb, const char *key);
int kvdb_get_key_ts(KVDB *kvdb, const char *key, char *insert_ts, size_t insert_ts_size, char *update_ts, size_t update_ts_size);
#endif
