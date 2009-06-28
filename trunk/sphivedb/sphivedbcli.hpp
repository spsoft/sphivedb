/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#ifndef __sphivedbcli_hpp__
#define __sphivedbcli_hpp__

#include <sys/types.h>

class SP_NKSocket;
class SP_NKStringList;

class SP_HiveRespObject;

class SP_NKEndPointTableConfig;
class SP_NKSocketPoolConfig;

typedef struct tagSP_HiveDBClientConfigImpl SP_HiveDBClientConfigImpl_t;
typedef struct tagSP_HiveDBClientImpl SP_HiveDBClientImpl_t;

class SP_HiveDBClientConfig {
public:
	SP_HiveDBClientConfig();
	~SP_HiveDBClientConfig();

	int init( const char * configFile );

	SP_NKEndPointTableConfig * getEndPointTableConfig();

	SP_NKSocketPoolConfig * getSocketPoolConfig();

private:
	SP_HiveDBClientConfigImpl_t * mImpl;
};

class SP_HiveDBClient {
public:
	SP_HiveDBClient();
	~SP_HiveDBClient();

	int init( const char * configFile );

	SP_HiveRespObject * execute( int dbfile, const char * user, const char * dbname,
			SP_NKStringList * sql );

private:
	SP_NKSocket * getSocket( int dbfile );

private:
	SP_HiveDBClientImpl_t * mImpl;
};

class SP_HiveDBProtocol {
public:

	SP_HiveDBProtocol( SP_NKSocket * socket, int isKeepAlive );

	SP_HiveDBProtocol();

	SP_HiveRespObject * execute( int dbfile, const char * user, const char * dbname,
			SP_NKStringList * sql );

private:
	SP_NKSocket * mSocket;
	int mIsKeepAlive;
};

#endif

