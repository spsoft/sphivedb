/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#ifndef __sphiveconfig_hpp__
#define __sphiveconfig_hpp__

class SP_NKNameValueList;
class SP_NKServerConfig;

class SP_HiveConfig {
public:
	SP_HiveConfig();
	~SP_HiveConfig();

	int init( const char * configFile );

	SP_NKServerConfig * getServerConfig();

	const char * getDataDir();
	int getDBFileBegin();
	int getDBFileEnd();
	int getMaxOpenFiles();
	int getMaxOpenDBs();

	int getLockTimeoutSeconds();

	const char * getDDL( const char * dbname );

	SP_NKNameValueList * getListOfDDL();

private:
	SP_NKServerConfig * mServerConfig;

	char mDataDir[ 256 ];
	int mDBFileBegin;
	int mDBFileEnd;
	int mMaxOpenFiles;
	int mMaxOpenDBs;

	int mLockTimeoutSeconds;

	SP_NKNameValueList * mListOfDDL;
};

#endif

