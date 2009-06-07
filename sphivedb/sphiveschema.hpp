/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#ifndef __sphiveschema_hpp__
#define __sphiveschema_hpp__

typedef struct sqlite3 sqlite3;

class SP_HiveConfig;
class SP_HiveDDLConfig;

class SP_HiveSchemaManager {
public:
	SP_HiveSchemaManager( SP_HiveConfig * config );
	~SP_HiveSchemaManager();

	int ensureSchema( sqlite3 * handle, const char * dbname );

private:
	static int execWithLog( sqlite3 * handle, const char * sql );

	static int updateIfNeed( sqlite3 * handle, const SP_HiveDDLConfig * ddl,
			const char * oldSql );

	// return 0 : OK, 1 : no record, -1 : error
	static int getTableSql( sqlite3 * handle, const char * table, char ** sql );

	SP_HiveConfig * mConfig;
};

#endif

