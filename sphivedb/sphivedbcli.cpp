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

#include "spjson/spjsonrpc.hpp"
#include "spjson/spjsonnode.hpp"
#include "spjson/spjsonutils.hpp"

SP_HiveDBProtocol :: SP_HiveDBProtocol( SP_NKSocket * socket, int isKeepAlive )
{
	mSocket = socket;
	mIsKeepAlive = isKeepAlive;
}

SP_HiveDBProtocol :: SP_HiveDBProtocol()
{
}

SP_HiveRespObject * SP_HiveDBProtocol :: execute( int dbfile, const char * user,
		const char * dbname, SP_NKStringList * sql )
{
	SP_JsonArrayNode params;
	{
		SP_JsonObjectNode * args = new SP_JsonObjectNode();

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

	SP_NKHttpRequest httpReq;
	{
		httpReq.setMethod( "POST" );
		httpReq.setURI( "/sphivedb" );
		httpReq.setVersion( "HTTP/1.1" );
		if( mIsKeepAlive ) httpReq.addHeader( "Connection", "Keep-Alive" );
		httpReq.addHeader( "Host", "127.0.0.1" );

		httpReq.appendContent( buffer.getBuffer(), buffer.getSize() );
	}

	SP_HiveRespObject * resp = NULL;

	SP_NKHttpResponse httpResp;

	int ret = SP_NKHttpProtocol::post( mSocket, &httpReq, &httpResp );
	if( 0 == ret ) {
		SP_JsonRpcRespObject * inner = new SP_JsonRpcRespObject(
				(char*)httpResp.getContent(), httpResp.getContentLength() );
		resp = new SP_HiveRespObject( inner, 1 );
	}

	return resp;
}

