/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#ifndef __sphiveconfig_hpp__
#define __sphiveconfig_hpp__

class SP_NKNameValueList;
class SP_NKVector;

class SP_HiveDDLConfig;

class SP_HiveConfig {
public:
	SP_HiveConfig();
	~SP_HiveConfig();

	int init( const char * configFile );

	int getMaxConnections();
	int getSocketTimeout();
	int getMaxThreads();
	int getMaxReqQueueSize();

	const char * getDataDir();
	int getDBFileBegin();
	int getDBFileEnd();
	int getMaxOpenFiles();

	int getLockTimeoutSeconds();

	const SP_HiveDDLConfig * getDDL( const char * dbname );

private:
	int mMaxConnections;
	int mSocketTimeout;
	int mMaxThreads;
	int mMaxReqQueueSize;

	char mDataDir[ 256 ];
	int mDBFileBegin;
	int mDBFileEnd;
	int mMaxOpenFiles;

	int mLockTimeoutSeconds;

	SP_NKVector * mListOfDDL;
};

class SP_HiveDDLConfig {
public:
	static int computeColumnCount( const char * sql );

public:
	SP_HiveDDLConfig();
	~SP_HiveDDLConfig();

	int init( const char * name, const char * sql );

	const char * getName() const;
	const char * getSql() const;

	const char * getTable() const;

	int getColumnCount() const;

	int findColumn( const char * name );

	const char * getColumnName( int index ) const;

	const char * getColumnType( int index ) const;

private:
	char * mName, * mSql, * mTable;
	SP_NKNameValueList * mColumnList;
};

#endif

