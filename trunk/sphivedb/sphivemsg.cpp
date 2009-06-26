/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#include <stdio.h>

#include "sphivemsg.hpp"

#include "sphivecomm.hpp"

#include "spjson/spjsonnode.hpp"
#include "spjson/spjsonrpc.hpp"
#include "spjson/spjsonhandle.hpp"

SP_HiveReqObject :: SP_HiveReqObject( SP_JsonRpcReqObject * inner )
{
	mInner = inner;
}

SP_HiveReqObject :: ~SP_HiveReqObject()
{
}

const char * SP_HiveReqObject :: verify()
{
	const char * ret = NULL;

	if( getDBFile() < 0 ) {
		ret = "invalid params, not dbfile property";
	} else if( NULL == getUser() ) {
		ret = "invalid params, not user property";
	} else if( NULL == getDBName() ) {
		ret = "invalid params, not dbname property";
	} else if( NULL == getSql(0) ) {
		ret = "invalid params, not sql property";
	}

	return ret;
}

int SP_HiveReqObject :: getDBFile()
{
	SP_JsonHandle handle( mInner->getParams() );

	SP_JsonIntNode * node = handle.getChild( 0 ).getChild( "dbfile" ).toInt();

	return NULL != node ? node->getValue() : -1;
}

const char * SP_HiveReqObject :: getUser()
{
	SP_JsonHandle handle( mInner->getParams() );

	SP_JsonStringNode * node = handle.getChild( 0 ).getChild( "user" ).toString();

	return NULL != node ? node->getValue() : NULL;
}

const char * SP_HiveReqObject :: getDBName()
{
	SP_JsonHandle handle( mInner->getParams() );

	SP_JsonStringNode * node = handle.getChild( 0 ).getChild( "dbname" ).toString();

	return NULL != node ? node->getValue() : NULL;
}

int SP_HiveReqObject :: getSqlCount()
{
	SP_JsonHandle handle( mInner->getParams() );

	SP_JsonArrayNode * node = handle.getChild( 0 ).getChild( "sql" ).toArray();

	return NULL != node ? node->getCount() : 0;
}

const char * SP_HiveReqObject :: getSql( int index )
{
	SP_JsonHandle handle( mInner->getParams() );

	SP_JsonStringNode * node = handle.getChild( 0 ).getChild( "sql" ).getChild( index ).toString();

	return NULL != node ? node->getValue() : NULL;
}

//====================================================================

SP_HiveRespObject :: SP_HiveRespObject( SP_JsonRpcRespObject * inner, int toBeOwner )
{
	mInner = inner;
	mToBeOwner = toBeOwner;
}

SP_HiveRespObject :: ~SP_HiveRespObject()
{
	if( mToBeOwner ) delete mInner, mInner = NULL;
}

int SP_HiveRespObject :: getErrorCode()
{
	SP_JsonHandle handle( mInner->getError() );

	SP_JsonIntNode * node = handle.getChild( "code" ).toInt();

	return NULL != node ? node->getValue() : 0;
}

const char * SP_HiveRespObject :: getErrorMsg()
{
	SP_JsonHandle handle( mInner->getError() );

	SP_JsonStringNode * node = handle.getChild( "message" ).toString();

	return NULL != node ? node->getValue() : NULL;
}

int SP_HiveRespObject :: getErrdataCode()
{
	SP_JsonHandle handle( mInner->getError() );

	SP_JsonIntNode * node = handle.getChild( "data" ).getChild( "code" ).toInt();

	return NULL != node ? node->getValue() : 0;
}

const char * SP_HiveRespObject :: getErrdataMsg()
{
	SP_JsonHandle handle( mInner->getError() );

	SP_JsonStringNode * node = handle.getChild( "data" ).getChild( "message" ).toString();

	return NULL != node ? node->getValue() : NULL;
}

int SP_HiveRespObject :: getResultCount()
{
	SP_JsonHandle handle( mInner->getResult() );

	SP_JsonArrayNode * node = handle.toArray();

	return NULL != node ? node->getCount() : 0;
}

SP_HiveResultSet * SP_HiveRespObject :: getResultSet( int index )
{
	SP_JsonHandle handle( mInner->getResult() );

	SP_JsonObjectNode * node = handle.getChild( index ).toObject();

	return NULL != node ? ( new SP_HiveResultSet( node ) ) : NULL;
}

