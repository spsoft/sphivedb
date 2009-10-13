/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#include <stdio.h>
#include <string.h>

#include "sphivehandler.hpp"

#include "sphivemanager.hpp"
#include "sphivemsg.hpp"

#include "spserver/sphttp.hpp"
#include "spserver/sphttpmsg.hpp"

#include "spjson/spjsonrpc.hpp"
#include "spjson/spjsonutils.hpp"
#include "spjson/spjsonnode.hpp"

SP_HiveHandler :: SP_HiveHandler( SP_HiveManager * manager )
{
	mManager = manager;
}

SP_HiveHandler :: ~SP_HiveHandler()
{
}

void SP_HiveHandler :: handle( SP_HttpRequest * request, SP_HttpResponse * response )
{
	SP_JsonRpcReqObject rpcReq( (char*)request->getContent(), request->getContentLength() );

	int ret = -1;

	SP_JsonNode * result = NULL;
	SP_JsonObjectNode error;

	if( NULL == rpcReq.getPacketError() ) {
		if( 0 == strcasecmp( rpcReq.getMethod(), "execute" ) ) {
			ret = doExecute( &rpcReq, &result, &error );
		} else if( 0 == strcasecmp( rpcReq.getMethod(), "remove" ) ) {
			ret = doRemove( &rpcReq, &result, &error );
		} else {
			SP_JsonRpcUtils::setError( &error,
					SP_JsonRpcUtils::eMethodNoFound, "Method not found." );
		}
	} else {
		SP_JsonRpcUtils::setError( &error,
				SP_JsonRpcUtils::eInvalidRequest, rpcReq.getPacketError() );
	}

	SP_JsonStringBuffer respBuffer;

	if( 0 == ret ) {
		SP_JsonRpcUtils::toRespBuffer( rpcReq.getID(), result,
				NULL, &respBuffer );
	} else {
		SP_JsonRpcUtils::toRespBuffer( rpcReq.getID(), NULL,
				&error, &respBuffer );
	}

	if( NULL != result ) delete result;

	response->setStatusCode( 200 );

	int len = respBuffer.getSize();
	response->directSetContent( respBuffer.takeBuffer(), len );
}

int SP_HiveHandler :: doExecute( SP_JsonRpcReqObject * rpcReq, SP_JsonNode ** result,
			SP_JsonObjectNode * error )
{
	int ret = -1;

	SP_HiveReqObjectJson reqObject( rpcReq );

	const char * errmsg = reqObject.verify();

	SP_JsonObjectNode * errdata = new SP_JsonObjectNode();
	{
		SP_JsonPairNode * dataPair = new SP_JsonPairNode();
		dataPair->setName( "data" );
		dataPair->setValue( errdata );

		error->addValue( dataPair );
	}

	if( NULL == errmsg ) {
		*result = new SP_JsonArrayNode();
		ret = mManager->execute( &reqObject, (SP_JsonArrayNode*)*result, errdata );
		if( 0 != ret ) {
			SP_JsonRpcUtils::setError( error,
				SP_JsonRpcUtils::eInternalError, "Internal error." );
		}
	} else {
		SP_JsonRpcUtils::setError( error, SP_JsonRpcUtils::eInvalidParams, errmsg );
	}

	return ret;
}

int SP_HiveHandler :: doRemove( SP_JsonRpcReqObject * rpcReq, SP_JsonNode ** result,
		SP_JsonObjectNode * error )
{
	int ret = -1;

	SP_HiveReqObjectJson reqObject( rpcReq );

	const char * errmsg = reqObject.verifyWithoutSql();

	SP_JsonObjectNode * errdata = new SP_JsonObjectNode();
	{
		SP_JsonPairNode * dataPair = new SP_JsonPairNode();
		dataPair->setName( "data" );
		dataPair->setValue( errdata );

		error->addValue( dataPair );
	}

	if( NULL == errmsg ) {
		ret = mManager->remove( &reqObject, errdata );
		if( ret < 0 ) {
			SP_JsonRpcUtils::setError( error,
				SP_JsonRpcUtils::eInternalError, "Internal error." );
		} else {
			*result = new SP_JsonIntNode( ret );
			ret = 0;
		}
	} else {
		SP_JsonRpcUtils::setError( error, SP_JsonRpcUtils::eInvalidParams, errmsg );
	}

	return ret;
}

//====================================================================

SP_HiveHandlerFactory :: SP_HiveHandlerFactory( SP_HiveManager * manager )
{
	mManager = manager;
}

SP_HiveHandlerFactory :: ~SP_HiveHandlerFactory()
{
}

SP_HttpHandler * SP_HiveHandlerFactory :: create() const
{
	return new SP_HiveHandler( mManager );
}

