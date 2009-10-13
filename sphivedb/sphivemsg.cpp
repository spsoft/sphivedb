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

SP_HiveReqObject :: ~SP_HiveReqObject()
{
}

SP_HiveRespObject :: ~SP_HiveRespObject()
{
}

//====================================================================

SP_HiveReqObjectJson :: SP_HiveReqObjectJson( SP_JsonRpcReqObject * inner )
{
	mInner = inner;
	mUniqKey[0] = '\0';
}

SP_HiveReqObjectJson :: ~SP_HiveReqObjectJson()
{
}

const char * SP_HiveReqObjectJson :: verify()
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

const char * SP_HiveReqObjectJson :: verifyWithoutSql()
{
	const char * ret = NULL;

	if( getDBFile() < 0 ) {
		ret = "invalid params, not dbfile property";
	} else if( NULL == getUser() ) {
		ret = "invalid params, not user property";
	} else if( NULL == getDBName() ) {
		ret = "invalid params, not dbname property";
	}

	return ret;
}

int SP_HiveReqObjectJson :: getDBFile()
{
	SP_JsonHandle handle( mInner->getParams() );

	SP_JsonIntNode * node = handle.getChild( 0 ).getChild( "dbfile" ).toInt();

	return NULL != node ? node->getValue() : -1;
}

const char * SP_HiveReqObjectJson :: getUser()
{
	SP_JsonHandle handle( mInner->getParams() );

	SP_JsonStringNode * node = handle.getChild( 0 ).getChild( "user" ).toString();

	return NULL != node ? node->getValue() : NULL;
}

const char * SP_HiveReqObjectJson :: getDBName()
{
	SP_JsonHandle handle( mInner->getParams() );

	SP_JsonStringNode * node = handle.getChild( 0 ).getChild( "dbname" ).toString();

	return NULL != node ? node->getValue() : NULL;
}

int SP_HiveReqObjectJson :: getSqlCount()
{
	SP_JsonHandle handle( mInner->getParams() );

	SP_JsonArrayNode * node = handle.getChild( 0 ).getChild( "sql" ).toArray();

	return NULL != node ? node->getCount() : 0;
}

const char * SP_HiveReqObjectJson :: getSql( int index )
{
	SP_JsonHandle handle( mInner->getParams() );

	SP_JsonStringNode * node = handle.getChild( 0 ).getChild( "sql" ).getChild( index ).toString();

	return NULL != node ? node->getValue() : NULL;
}

const char * SP_HiveReqObjectJson :: getUniqKey()
{
	if( '\0' == mUniqKey[0] ) {
		snprintf( mUniqKey, sizeof( mUniqKey ), "%d/%s/%s", getDBFile(), getUser(), getDBName() );
	}

	return mUniqKey;
}

//====================================================================

SP_HiveRespObjectJson :: SP_HiveRespObjectJson( SP_JsonRpcRespObject * inner, int toBeOwner )
{
	mInner = inner;
	mToBeOwner = toBeOwner;
}

SP_HiveRespObjectJson :: ~SP_HiveRespObjectJson()
{
	if( mToBeOwner ) delete mInner, mInner = NULL;
}

int SP_HiveRespObjectJson :: getErrorCode()
{
	SP_JsonHandle handle( mInner->getError() );

	SP_JsonIntNode * node = handle.getChild( "code" ).toInt();

	return NULL != node ? node->getValue() : 0;
}

const char * SP_HiveRespObjectJson :: getErrorMsg()
{
	SP_JsonHandle handle( mInner->getError() );

	SP_JsonStringNode * node = handle.getChild( "message" ).toString();

	return NULL != node ? node->getValue() : NULL;
}

int SP_HiveRespObjectJson :: getErrdataCode()
{
	SP_JsonHandle handle( mInner->getError() );

	SP_JsonIntNode * node = handle.getChild( "data" ).getChild( "code" ).toInt();

	return NULL != node ? node->getValue() : 0;
}

const char * SP_HiveRespObjectJson :: getErrdataMsg()
{
	SP_JsonHandle handle( mInner->getError() );

	SP_JsonStringNode * node = handle.getChild( "data" ).getChild( "message" ).toString();

	return NULL != node ? node->getValue() : NULL;
}

int SP_HiveRespObjectJson :: getResultCount()
{
	SP_JsonHandle handle( mInner->getResult() );

	SP_JsonArrayNode * node = handle.toArray();

	return NULL != node ? node->getCount() : 0;
}

SP_HiveResultSet * SP_HiveRespObjectJson :: getResultSet( int index )
{
	SP_JsonHandle handle( mInner->getResult() );

	SP_JsonObjectNode * node = handle.getChild( index ).toObject();

	return NULL != node ? ( new SP_HiveResultSetJson( node ) ) : NULL;
}

