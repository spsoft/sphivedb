/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdio.h>

#include "sphivedbcli.hpp"
#include "sphivemsg.hpp"

#include "spnetkit/spnkhttpcli.hpp"
#include "spnetkit/spnkhttpmsg.hpp"
#include "spnetkit/spnklist.hpp"
#include "spnetkit/spnkini.hpp"
#include "spnetkit/spnkendpoint.hpp"
#include "spnetkit/spnkconfig.hpp"
#include "spnetkit/spnksocketpool.hpp"
#include "spnetkit/spnklog.hpp"
#include "spnetkit/spnksocket.hpp"

#include "spjson/spjsonrpc.hpp"
#include "spjson/spjsonnode.hpp"
#include "spjson/spjsonutils.hpp"
#include "spjson/spjsonhandle.hpp"

typedef struct tagSP_HiveDBClientConfigImpl {
	SP_NKEndPointTableConfig * mEndPointTableConfig;
	SP_NKSocketPoolConfig * mSocketPoolConfig;
} SP_HiveDBClientConfigImpl_t;

SP_HiveDBClientConfig :: SP_HiveDBClientConfig()
{
	mImpl = (SP_HiveDBClientConfigImpl_t*)calloc( sizeof( SP_HiveDBClientConfigImpl_t ), 1 );
}

SP_HiveDBClientConfig :: ~SP_HiveDBClientConfig()
{
	if( mImpl->mEndPointTableConfig ) delete mImpl->mEndPointTableConfig;
	if( mImpl->mSocketPoolConfig ) delete mImpl->mSocketPoolConfig;

	free( mImpl ), mImpl = NULL;
}

int SP_HiveDBClientConfig :: init( const char * configFile )
{
	int ret = -1;

	mImpl->mSocketPoolConfig = new SP_NKSocketPoolConfig();
	mImpl->mEndPointTableConfig = new SP_NKEndPointTableConfig();

	SP_NKIniFile iniFile;
	if( 0 == iniFile.open( configFile ) ) {
		ret = mImpl->mSocketPoolConfig->init( &iniFile, "SocketPool" );

		ret |= mImpl->mEndPointTableConfig->init( &iniFile );
	}

	return ret;
}

SP_NKEndPointTableConfig * SP_HiveDBClientConfig :: getEndPointTableConfig()
{
	return mImpl->mEndPointTableConfig;
}

SP_NKSocketPoolConfig * SP_HiveDBClientConfig :: getSocketPoolConfig()
{
	return mImpl->mSocketPoolConfig;
}

//====================================================================

typedef struct tagSP_HiveDBClientImpl {
	SP_HiveDBClientConfig * mConfig;
	SP_NKSocketPool * mSocketPool;
} SP_HiveDBClientImpl_t;

SP_HiveDBClient :: SP_HiveDBClient()
{
	mImpl = (SP_HiveDBClientImpl_t*)calloc( sizeof( SP_HiveDBClientImpl_t ), 1 );
}

SP_HiveDBClient :: ~SP_HiveDBClient()
{
	if( mImpl->mConfig ) delete mImpl->mConfig;
	if( mImpl->mSocketPool ) delete mImpl->mSocketPool;

	free( mImpl ), mImpl = NULL;
}

int SP_HiveDBClient :: init( const char * configFile )
{
	mImpl->mConfig = new SP_HiveDBClientConfig();

	int ret = mImpl->mConfig->init( configFile );

	if( 0 == ret ) {
		SP_NKSocketPoolConfig * config = mImpl->mConfig->getSocketPoolConfig();

		SP_NKTcpSocketFactory * factory = new SP_NKTcpSocketFactory();
		factory->setTimeout( config->getConnectTimeout(), config->getSocketTimeout() );

		SP_NKSocketPool * socketPool = new SP_NKSocketPool( config->getMaxIdlePerEndPoint(), factory );
		socketPool->setMaxIdleTime( config->getMaxIdleTime() );

		mImpl->mSocketPool = socketPool;
	}

	return ret;
}

SP_NKSocket * SP_HiveDBClient :: getSocket( int dbfile )
{
	SP_NKSocket * socket = NULL;

	SP_NKEndPoint_t ep;

	if( 0 == mImpl->mConfig->getEndPointTableConfig()->getEndPoint( dbfile, &ep ) ) {
		socket = mImpl->mSocketPool->get( ep.mIP, ep.mPort );
	} else {
		SP_NKLog::log( LOG_WARNING, "Cannot found endpoint for dbfile %d", dbfile );
	}

	return socket;
}

SP_HiveRespObject * SP_HiveDBClient :: execute( int dbfile, const char * user,
		const char * dbname, SP_NKStringList * sql )
{
	SP_HiveRespObject * resp = NULL;

	SP_NKSocket * socket = getSocket( dbfile );
	if( NULL != socket ) {
		SP_HiveDBProtocol protocol( socket, 1 );

		resp = protocol.execute( dbfile, user, dbname, sql );

		if( NULL != resp ) {
			mImpl->mSocketPool->save( socket );
		} else {
			delete socket;
		}
	}

	return resp;
}

//====================================================================

SP_HiveDBProtocol :: SP_HiveDBProtocol( SP_NKSocket * socket, int isKeepAlive )
{
	mSocket = socket;
	mIsKeepAlive = isKeepAlive;
}

SP_HiveDBProtocol :: ~SP_HiveDBProtocol()
{
}

int SP_HiveDBProtocol :: makeArgs( SP_JsonObjectNode * args, int dbfile, const char * user,
		const char * dbname )
{
	SP_JsonPairNode * dbfilePair = new SP_JsonPairNode();
	dbfilePair->setName( "dbfile" );
	dbfilePair->setValue( new SP_JsonIntNode( dbfile ) );

	args->addValue( dbfilePair );

	SP_JsonPairNode * userPair = new SP_JsonPairNode();
	userPair->setName( "user" );
	userPair->setValue( new SP_JsonStringNode( user ) );

	args->addValue( userPair );

	SP_JsonPairNode * dbnamePair = new SP_JsonPairNode();
	dbnamePair->setName( "dbname" );
	dbnamePair->setValue( new SP_JsonStringNode( dbname ) );

	args->addValue( dbnamePair );

	return 0;
}

int SP_HiveDBProtocol :: clientCall( SP_NKSocket * socket, int isKeepAlive,
		SP_JsonStringBuffer * reqBuff, SP_NKHttpResponse * httpResp )
{
	SP_NKHttpRequest httpReq;
	{
		httpReq.setMethod( "POST" );
		httpReq.setURI( "/sphivedb" );
		httpReq.setVersion( "HTTP/1.1" );
		if( isKeepAlive ) httpReq.addHeader( "Connection", "Keep-Alive" );
		httpReq.addHeader( "Host", "127.0.0.1" );

		httpReq.appendContent( reqBuff->getBuffer(), reqBuff->getSize() );
	}

	return SP_NKHttpProtocol::post( socket, &httpReq, httpResp );
}

SP_HiveRespObject * SP_HiveDBProtocol :: execute( int dbfile, const char * user,
		const char * dbname, SP_NKStringList * sql )
{
	SP_JsonArrayNode params;
	{
		SP_JsonObjectNode * args = new SP_JsonObjectNode();

		makeArgs( args, dbfile, user, dbname );

		SP_JsonArrayNode * sqlNode = new SP_JsonArrayNode();

		for( int i = 0; i < sql->getCount(); i++ ) {
			sqlNode->addValue( new SP_JsonStringNode( sql->getItem( i ) ) );
		}

		SP_JsonPairNode * sqlPair = new SP_JsonPairNode();
		sqlPair->setName( "sql" );
		sqlPair->setValue( sqlNode );

		args->addValue( sqlPair );

		params.addValue( args );
	}

	SP_JsonStringBuffer buffer;

	SP_JsonRpcUtils::toReqBuffer( "execute", user, &params, &buffer );

	SP_HiveRespObject * resp = NULL;

	SP_NKHttpResponse httpResp;

	int ret = clientCall( mSocket, mIsKeepAlive, &buffer, &httpResp );

	if( 0 == ret ) {
		SP_JsonRpcRespObject * inner = new SP_JsonRpcRespObject(
				(char*)httpResp.getContent(), httpResp.getContentLength() );
		resp = new SP_HiveRespObjectJson( inner, 1 );
	} else {
		SP_NKLog::log( LOG_WARNING, "clientCall %d", ret );
	}

	return resp;
}

int SP_HiveDBProtocol :: remove( int dbfile, const char * user,
		const char * dbname )
{
	SP_JsonArrayNode params;
	{
		SP_JsonObjectNode * args = new SP_JsonObjectNode();

		makeArgs( args, dbfile, user, dbname );

		params.addValue( args );
	}

	SP_JsonStringBuffer buffer;

	SP_JsonRpcUtils::toReqBuffer( "remove", user, &params, &buffer );

	SP_NKHttpResponse httpResp;

	int ret = clientCall( mSocket, mIsKeepAlive, &buffer, &httpResp );

	if( 0 == ret ) {
		SP_JsonRpcRespObject respObj( (char*)httpResp.getContent(),
				httpResp.getContentLength() );

		SP_JsonHandle handle( respObj.getResult() );
		SP_JsonIntNode * result = handle.toInt();

		ret = ( NULL != result ) ? result->getValue() : -1;
	}

	return ret;
}

