#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "kvdb.h"

/* Open KV database */
int kvdb_open_db(KVDB *kvdb, const char *filename) {
	if (sqlite3_open(filename, &(kvdb->db)) != SQLITE_OK) {
		return KVDB_RETURN_ERROR;
	}
	return KVDB_RETURN_OK;
}

/* Close KV database */
int kvdb_close_db(KVDB *kvdb) {
	if (sqlite3_close(kvdb->db) != SQLITE_OK) {
		return KVDB_RETURN_ERROR;
	}
	return KVDB_RETURN_OK;
}

/* Create KV table, if not available */
int kvdb_create_table_in_db(KVDB *kvdb) {
	char *err_msg = NULL;
	const char *sql = "CREATE TABLE IF NOT EXISTS kv ("
				"key TEXT PRIMARY KEY"
				",value TEXT"
				",insert_ts TEXT DEFAULT(strftime('%Y-%m-%d %H:%M:%fZ', 'now' ))"
				",update_ts TEXT DEFAULT(strftime('%Y-%m-%d %H:%M:%fZ', 'now' ))"
			") WITHOUT ROWID"
			";";

	if (sqlite3_exec(kvdb->db, sql, 0, 0, &err_msg) != SQLITE_OK) {
		sqlite3_free(err_msg);
		return KVDB_RETURN_ERROR;
	}
	return KVDB_RETURN_OK;
}

/* Set key: Insert or update a key-value pair in table */
int kvdb_set_key(KVDB *kvdb, const char *key, const char *value) {
	sqlite3_stmt *stmt;
	const char *sql = "INSERT INTO kv(key, value) VALUES (?, ?) "
				"ON CONFLICT (key) DO "
				"UPDATE SET value = excluded.value, update_ts = strftime('%Y-%m-%d %H:%M:%fZ', 'now' )"
				";"; /* create_ts remains unchanged, even when value changes. */

	if (sqlite3_prepare_v2(kvdb->db, sql, -1, &stmt, 0) != SQLITE_OK) {
		return KVDB_RETURN_ERROR;
	}
	sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
	sqlite3_bind_text(stmt, 2, value, -1, SQLITE_STATIC);

	if (sqlite3_step(stmt) != SQLITE_DONE) {
		sqlite3_finalize(stmt);
		return KVDB_RETURN_ERROR;
	}
	sqlite3_finalize(stmt);
	return KVDB_RETURN_OK;
}

/* Get key value: Select a row from table */
const char *kvdb_get_key_value(KVDB *kvdb, const char *key) {
	sqlite3_stmt *stmt;
	const char *sql = "SELECT value FROM kv WHERE key = ?;";
	const char *key_value = NULL;

	if (sqlite3_prepare_v2(kvdb->db, sql, -1, &stmt, 0) != SQLITE_OK) {
		return NULL;
	}
	sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
	if (sqlite3_step(stmt) == SQLITE_ROW) {
		key_value = strdup((const char *) sqlite3_column_text(stmt, 0));
	}
	sqlite3_finalize(stmt);
	return key_value;
}

/* Delete key: Delete a row from table */ 
int kvdb_del_key(KVDB *kvdb, const char *key) {
	sqlite3_stmt *stmt;
	const char *sql = "DELETE FROM kv WHERE key = ?;";
	if (sqlite3_prepare_v2(kvdb->db, sql, -1, &stmt, 0) != SQLITE_OK) {
		return KVDB_RETURN_ERROR;
	}
	sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
	if (sqlite3_step(stmt) != SQLITE_DONE) {
		sqlite3_finalize(stmt);
		return KVDB_RETURN_ERROR;
	}
	sqlite3_finalize(stmt);
	return KVDB_RETURN_OK;
}

/* Get key timestamps: Select timestamp columns fro table */
int kvdb_get_key_ts(KVDB *kvdb, const char *key, char *insert_ts, size_t insert_ts_size, char *update_ts, size_t update_ts_size) {
	sqlite3_stmt *stmt;
	const char *insert_ts_column = NULL;
	const char *update_ts_column = NULL;

	const char *sql = "SELECT insert_ts, update_ts FROM kv WHERE key = ?;";

	if (sqlite3_prepare_v2(kvdb->db, sql, -1, &stmt, 0) != SQLITE_OK) {
		return KVDB_RETURN_ERROR;
	}
	sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
	if (sqlite3_step(stmt) == SQLITE_ROW) {
		insert_ts_column = (const char *) sqlite3_column_text(stmt, 0);
		strncpy(insert_ts, insert_ts_column, insert_ts_size - 1);
		insert_ts[insert_ts_size - 1] = '\0';

		update_ts_column = (const char *) sqlite3_column_text(stmt, 1);
		strncpy(update_ts, update_ts_column, update_ts_size - 1);
		update_ts[update_ts_size - 1] = '\0';

	} else {
		sqlite3_finalize(stmt);
		return KVDB_RETURN_ERROR;
	}
	sqlite3_finalize(stmt);
	return KVDB_RETURN_OK;
}

void usage(const char *exec_name) {
	fprintf(stderr, "Usage: %s <action> [<key>] [<value>]\n", exec_name);
	fprintf(stderr, "Actions: \n");
	fprintf(stderr, "\t set <key> <value>: Associate <key> with <value>, record timestamp of creation and/or last update\n");
	fprintf(stderr, "\t get <key>: Fetch the value associated with <key>\n");
	fprintf(stderr, "\t del <key>: Remove <key> from the database\n");
	fprintf(stderr, "\t ts <key>: Fetch the timestamps when <key> was first and last set\n");
}

int main(int argc, char *argv[]) {
	KVDB kvdb;
	const char *key_value;
	char insert_ts[KVDB_TS_CHAR_LENGTH];
	char update_ts[KVDB_TS_CHAR_LENGTH];

	/* Check input parameters for correctness, display usage otherwise */
	if (
		(argc != 4 && argc != 3) ||
		(argc == 4 && strcmp(argv[1], "set") != 0) ||
		(argc == 3 && strcmp(argv[1], "get") != 0 && strcmp(argv[1], "del") != 0 && strcmp(argv[1], "ts") != 0)
	   ) {
		usage(argv[0]);
		return EXIT_FAILURE;
	}
	/* Open the database */
	if (kvdb_open_db(&kvdb, KVDB_DB_FILENAME) != KVDB_RETURN_OK) {
		fprintf(stderr, "%s: Could not open database '%s'\n", argv[0], KVDB_DB_FILENAME);
		return EXIT_FAILURE;
	}
	/* Create key-value table, if not there */
	if (kvdb_create_table_in_db(&kvdb) != KVDB_RETURN_OK) {
		fprintf(stderr, "%s: Could not create/access table in database '%s'\n", argv[0], KVDB_DB_FILENAME);
		kvdb_close_db(&kvdb);
		return EXIT_FAILURE;
	}
	/* Perform requested action - Set key value */
	if (strcmp(argv[1], "set") == 0) {
		if (kvdb_set_key(&kvdb, argv[2], argv[3]) == KVDB_RETURN_OK) {
			fprintf(stdout, "%s: Set key '%s' value to '%s'. SUCCESS\n", argv[0], argv[2], argv[3]);
		} else {
			fprintf(stderr, "%s: Set key '%s' value to '%s'. FAIL\n", argv[0], argv[2], argv[3]);
			kvdb_close_db(&kvdb);
			return EXIT_FAILURE;
		}
	}
	/* Perform requested action - Get key value */
	if (strcmp(argv[1], "get") == 0) {
		key_value = kvdb_get_key_value(&kvdb, argv[2]);
		if ( key_value != NULL ) {
			fprintf(stdout, "%s: Get key '%s' returned value '%s'. SUCCESS\n", argv[0], argv[2], key_value);
		} else {
			fprintf(stdout, "%s: Get key '%s' returned no value. SUCCESS\n", argv[0], argv[2]);
		}
	}
	/* Perform requested action - Delete key */
	if (strcmp(argv[1], "del") == 0) {
		if (kvdb_del_key(&kvdb, argv[2]) == KVDB_RETURN_OK) {
			fprintf(stdout, "%s: Del key '%s'. SUCCESS\n", argv[0], argv[2]);
		} else {
			fprintf(stderr, "%s: Del key '%s'. FAIL\n", argv[0], argv[2]);
			kvdb_close_db(&kvdb);
			return EXIT_FAILURE;
		}
	}
	/* Perform requested action - Get key timestamps */
	if (strcmp(argv[1], "ts") == 0) {
		if (kvdb_get_key_ts(&kvdb, argv[2], insert_ts, sizeof(insert_ts), update_ts, sizeof(update_ts)) == KVDB_RETURN_OK) {
			fprintf(stdout, "%s: Get key '%s' timestamps: It was first set at %s and last at %s. SUCCESS\n", argv[0], argv[2], insert_ts, update_ts);
		} else {
			fprintf(stdout, "%s: Get key '%s' not found, no timestamps. FAIL\n", argv[0], argv[2]);
			kvdb_close_db(&kvdb);
			return EXIT_FAILURE;
		}
	}
	kvdb_close_db(&kvdb);
	return EXIT_SUCCESS;
}
