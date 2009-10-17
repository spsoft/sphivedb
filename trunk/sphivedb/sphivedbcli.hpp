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
class SP_NKHttpResponse;

class SP_JsonObjectNode;
class SP_JsonStringBuffer;
class SP_ProtoBufEncoder;

typedef struct tagSP_HiveDBClientConfigImpl SP_HiveDBClientConfigImpl_t;
typedef struct tagSP_HiveDBClientImpl SP_HiveDBClientImpl_t;

class SP_HiveDBClientConfig {
public:
	enum { eJsonRpc, eProtoBufRpc };

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

	int init( const char * configFile, int rpcType = SP_HiveDBClientConfig::eJsonRpc );

	SP_HiveRespObject * execute( int dbfile, const char * user, const char * dbname,
			SP_NKStringList * sql );

	int remove( int dbfile, const char * user, const char * dbname );

private:
	SP_NKSocket * getSocket( int dbfile );

private:
	SP_HiveDBClientImpl_t * mImpl;
};

class SP_HiveDBProtocol {
public:

	SP_HiveDBProtocol( SP_NKSocket * socket, int isKeepAlive, int rpcType );

	~SP_HiveDBProtocol();

	SP_HiveRespObject * execute( int dbfile, const char * user, const char * dbname,
			SP_NKStringList * sql );

	int remove( int dbfile, const char * user, const char * dbname, int * result );

private:

	SP_HiveRespObject * executeJson( int dbfile, const char * user, const char * dbname,
			SP_NKStringList * sql );

	int removeJson( int dbfile, const char * user, const char * dbname, int * result );

	static int makeArgs( SP_JsonObjectNode * args, int dbfile, const char * user,
			const char * dbname );

	SP_HiveRespObject * executeProtoBuf( int dbfile, const char * user, const char * dbname,
			SP_NKStringList * sql );

	int removeProtoBuf( int dbfile, const char * user, const char * dbname, int * result );

	static int makeArgs( SP_ProtoBufEncoder * args, int dbfile, const char * user,
			const char * dbname );

private:

	static int clientCall( SP_NKSocket * socket, const char * uri, int isKeepAlive,
			const char * reqBuff, int reqLen, SP_NKHttpResponse * httpResp );

private:
	SP_NKSocket * mSocket;
	int mIsKeepAlive;
	int mRpcType;
};

#endif

