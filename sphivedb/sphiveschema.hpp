/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#ifndef __sphiveschema_hpp__
#define __sphiveschema_hpp__

typedef struct sqlite3 sqlite3;

class SP_HiveConfig;
class SP_HiveTableSchema;
class SP_HiveDBSchema;

class SP_NKNameValueList;
class SP_NKVector;

class SP_HiveSchemaManager {
public:
	SP_HiveSchemaManager();
	~SP_HiveSchemaManager();

	int init( SP_HiveConfig * config );

	int ensureSchema( sqlite3 * handle, const char * dbname );

	static int execWithLog( sqlite3 * handle, const char * sql );

private:
	static int alterTable( sqlite3 * handle, SP_HiveTableSchema * newTable,
			SP_HiveTableSchema * oldTable );

	SP_HiveDBSchema * findDB( const char * dbname );

	SP_HiveConfig * mConfig;
	SP_NKVector * mDBList;
};

class SP_HiveDBSchema {
public:
	SP_HiveDBSchema();
	~SP_HiveDBSchema();

	int init( const char * dbname, const char * ddl );

	const char * getDBName() const;

	int getTableCount() const;
	SP_HiveTableSchema * getTable( int index ) const;

	int findTable( const char * tableName );

private:

	int createTable( sqlite3 * handle, const char * sql,
			char * table, int size );

private:
	char * mDBName;
	SP_NKVector * mTableList;
};

class SP_HiveTableSchema {
public:
	static int getTableColumnCount( sqlite3 * handle, const char * table );

public:
	SP_HiveTableSchema();
	~SP_HiveTableSchema();

	int init( sqlite3 * handle, const char * tableName, const char * ddl );

	const char * getTableName() const;

	const char * getDDL() const;

	int getColumnCount() const;

	int findColumn( const char * name );

	const char * getColumnName( int index ) const;

	const char * getColumnDefine( int index ) const;

private:
	char * mTableName;
	char * mDDL;
	SP_NKNameValueList * mColumnList;
};

#endif

