/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#ifndef __sphiveschema_hpp__
#define __sphiveschema_hpp__

typedef struct sqlite3 sqlite3;

class SP_HiveConfig;
class SP_HiveTableSchema;

class SP_NKNameValueList;
class SP_NKVector;

class SP_HiveSchemaManager {
public:
	SP_HiveSchemaManager();
	~SP_HiveSchemaManager();

	int init( SP_HiveConfig * config );

	int ensureSchema( sqlite3 * handle, const char * dbname );

private:
	static int execWithLog( sqlite3 * handle, const char * sql );

	static int alterTable( sqlite3 * handle, SP_HiveTableSchema * newTable,
			SP_HiveTableSchema * oldTable );

	static int createTable( sqlite3 * handle, const char * sql,
			char * table, int size );

	SP_HiveTableSchema * findTable( const char * dbname );

	SP_HiveConfig * mConfig;
	SP_NKVector * mTableList;
};

class SP_HiveTableSchema {
public:
	static int getTableColumnCount( sqlite3 * handle, const char * table );

public:
	SP_HiveTableSchema();
	~SP_HiveTableSchema();

	int init( sqlite3 * handle, const char * dbname, const char * table );

	const char * getDbname() const;

	const char * getTable() const;

	int getColumnCount() const;

	int findColumn( const char * name );

	const char * getColumnName( int index ) const;

	const char * getColumnDefine( int index ) const;

private:
	char * mDbname, * mTable;
	SP_NKNameValueList * mColumnList;
};

#endif

