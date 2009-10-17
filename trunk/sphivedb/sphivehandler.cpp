/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#include <stdio.h>
#include <string.h>

#include "sphivehandler.hpp"

#include "sphivemanager.hpp"
#include "sphivemsg.hpp"
#include "sphivejson.hpp"
#include "sphivepb.hpp"
#include "sphivegather.hpp"

#include "spserver/sphttp.hpp"
#include "spserver/sphttpmsg.hpp"

#include "spjson/spjsonrpc.hpp"
#include "spjson/spjsonutils.hpp"
#include "spjson/spjsonnode.hpp"
#include "spjson/sppbrpc.hpp"

SP_HiveHandler :: SP_HiveHandler( SP_HiveManager * manager )
{
	mManager = manager;
}

SP_HiveHandler :: ~SP_HiveHandler()
{
}

void SP_HiveHandler :: handle( SP_HttpRequest * request, SP_HttpResponse * response )
{
	if( 0 == strcasecmp( request->getURI(), "/sphivedb/protobuf" ) ) {
		handleProtoBuf( request, response );
	} else {
		handleJson( request, response );
	}
}

void SP_HiveHandler :: handleJson( SP_HttpRequest * request, SP_HttpResponse * response )
{
	SP_JsonRpcReqObject rpcReq( (char*)request->getContent(), request->getContentLength() );

	int ret = -1;

	SP_JsonNode * result = NULL;

	SP_HiveReqObjectJson reqObject( &rpcReq );
	SP_HiveRespObjectGatherJson respObject;

	if( NULL == rpcReq.getPacketError() ) {
		if( 0 == strcasecmp( rpcReq.getMethod(), "execute" ) ) {
			ret = doExecute( &reqObject, &respObject );
			result = respObject.getResult();
		} else if( 0 == strcasecmp( rpcReq.getMethod(), "remove" ) ) {
			ret = doRemove( &reqObject, &respObject );
			result = new SP_JsonIntNode( ret );
		} else {
			respObject.reportError( SP_JsonRpcUtils::eMethodNoFound, "Method not found." );
		}
	} else {
		respObject.reportError( SP_JsonRpcUtils::eInvalidRequest, rpcReq.getPacketError() );
	}

	SP_JsonStringBuffer respBuffer;

	if( 0 == ret ) {
		SP_JsonRpcUtils::toRespBuffer( rpcReq.getID(), result,
				NULL, &respBuffer );
	} else {
		SP_JsonRpcUtils::toRespBuffer( rpcReq.getID(), NULL,
				respObject.getError(), &respBuffer );
	}

	if( NULL != result && respObject.getResult() != result ) delete result;

	response->setStatusCode( 200 );

	int len = respBuffer.getSize();
	response->directSetContent( respBuffer.takeBuffer(), len );
}

void SP_HiveHandler :: handleProtoBuf( SP_HttpRequest * request, SP_HttpResponse * response )
{
	SP_ProtoBufRpcReqObject rpcReq;
	rpcReq.copyFrom( (char*)request->getContent(), request->getContentLength() );

	int ret = -1;

	SP_ProtoBufDecoder::KeyValPair_t result, id;
	memset( &result, 0, sizeof( result ) );
	rpcReq.getID( &id );

	SP_HiveReqObjectProtoBuf reqObject( &rpcReq );
	SP_HiveRespObjectGatherProtoBuf respObject;

	if( NULL == rpcReq.getPacketError() ) {
		if( 0 == strcasecmp( rpcReq.getMethod(), "execute" ) ) {
			ret = doExecute( &reqObject, &respObject );

			result.mWireType = SP_ProtoBufDecoder::eWireBinary;
			result.mBinary.mBuffer = (char*)respObject.getResult()->getBuffer();
			result.mBinary.mLen = respObject.getResult()->getSize();
		} else if( 0 == strcasecmp( rpcReq.getMethod(), "remove" ) ) {
			ret = doRemove( &reqObject, &respObject );

			result.mWireType = SP_ProtoBufDecoder::eWire32Bit;
			result.m32Bit.s = ret;
		} else {
			respObject.reportError( SP_JsonRpcUtils::eMethodNoFound, "Method not found." );
		}
	} else {
		respObject.reportError( SP_JsonRpcUtils::eInvalidRequest, rpcReq.getPacketError() );
	}

	SP_ProtoBufEncoder respEncoder;

	if( 0 == ret ) {
		SP_ProtoBufRpcUtils::initRespEncoder( &respEncoder, &id, NULL );

		SP_ProtoBufCodecUtils::addField( &respEncoder,
				SP_ProtoBufRpcRespObject::eResult, &result );
	} else {
		SP_ProtoBufRpcUtils::initRespEncoder( &respEncoder, &id,
				respObject.getError() );
	}

	response->setStatusCode( 200 );

	int len = respEncoder.getSize();
	response->directSetContent( respEncoder.takeBuffer(), len );
}

int SP_HiveHandler :: doExecute( SP_HiveReqObject * reqObject, SP_HiveRespObjectGather * respObject )
{
	int ret = -1;

	const char * errmsg = reqObject->verify();

	if( NULL == errmsg ) {
		ret = mManager->execute( reqObject, respObject );
		if( 0 != ret ) {
			respObject->reportError( SP_JsonRpcUtils::eInternalError, "Internal error." );
		}
	} else {
		respObject->reportError( SP_JsonRpcUtils::eInvalidParams, errmsg );
	}

	return ret;
}

int SP_HiveHandler :: doRemove( SP_HiveReqObject * reqObject, SP_HiveRespObjectGather * respObject )
{
	int ret = -1;

	const char * errmsg = reqObject->verifyWithoutSql();

	if( NULL == errmsg ) {
		ret = mManager->remove( reqObject, respObject );
		if( ret < 0 ) {
			respObject->reportError( SP_JsonRpcUtils::eInternalError, "Internal error." );
		} else {
			ret = 0;
		}
	} else {
		respObject->reportError( SP_JsonRpcUtils::eInvalidParams, errmsg );
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

