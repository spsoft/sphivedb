/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#include <stdio.h>

#include "sphivegather.hpp"

#include "spjson/spjsonnode.hpp"
#include "spjson/spjsonrpc.hpp"

SP_HiveResultSetGather :: ~SP_HiveResultSetGather()
{
}

SP_HiveRespObjectGather :: ~SP_HiveRespObjectGather()
{
}

//====================================================================

SP_HiveResultSetGatherJson :: SP_HiveResultSetGatherJson( SP_JsonObjectNode * inner )
{
	mInner = inner;

	mNameList = new SP_JsonArrayNode();
	{
		SP_JsonPairNode * pair = new SP_JsonPairNode();

		pair->setName( "name" );
		pair->setValue( mNameList );

		mInner->addValue( pair );
	}

	mTypeList = new SP_JsonArrayNode();
	{
		SP_JsonPairNode * pair = new SP_JsonPairNode();

		pair->setName( "type" );
		pair->setValue( mTypeList );

		mInner->addValue( pair );
	}

	mRowList = new SP_JsonArrayNode();
	{
		SP_JsonPairNode * pair = new SP_JsonPairNode();

		pair->setName( "row" );
		pair->setValue( mRowList );

		mInner->addValue( pair );
	}

	mRow = NULL;
}

SP_HiveResultSetGatherJson :: ~SP_HiveResultSetGatherJson()
{
}

int SP_HiveResultSetGatherJson :: addType( const char * type )
{
	mTypeList->addValue( new SP_JsonStringNode( type ) );

	return 0;
}

int SP_HiveResultSetGatherJson :: addName( const char * name )
{
	mNameList->addValue( new SP_JsonStringNode( name ) );

	return 0;
}

int SP_HiveResultSetGatherJson :: addRow()
{
	mRow = new SP_JsonArrayNode();

	mRowList->addValue( mRow );

	return 0;
}

int SP_HiveResultSetGatherJson :: addColumn( const char * value )
{
	if( NULL != value ) {
		mRow->addValue( new SP_JsonStringNode( value ) );
	} else {
		mRow->addValue( new SP_JsonNullNode() );
	}

	return 0;
}

int SP_HiveResultSetGatherJson :: addColumn( int value )
{
	mRow->addValue( new SP_JsonIntNode( value ) );

	return 0;
}

//====================================================================

SP_HiveRespObjectGatherJson :: SP_HiveRespObjectGatherJson()
{
	mResult = new SP_JsonArrayNode();
	mErrdata = new SP_JsonObjectNode();

	mResultSet = NULL;
}

SP_HiveRespObjectGatherJson :: ~SP_HiveRespObjectGatherJson()
{
	delete mResult;
	delete mErrdata;

	if( NULL != mResultSet ) delete mResultSet;
}

int SP_HiveRespObjectGatherJson :: addResultSet()
{
	SP_JsonObjectNode * inner = new SP_JsonObjectNode();
	mResult->addValue( inner );

	if( NULL != mResultSet ) delete mResultSet;

	mResultSet = new SP_HiveResultSetGatherJson( inner );

	return 0;
}

SP_HiveResultSetGather * SP_HiveRespObjectGatherJson :: getResultSet()
{
	return mResultSet;
}

int SP_HiveRespObjectGatherJson :: reportError( int errcode, const char * errmsg )
{
	SP_JsonRpcUtils::setError( mErrdata, errcode, errmsg );

	return 0;
}

SP_JsonArrayNode * SP_HiveRespObjectGatherJson :: getResult()
{
	return mResult;
}

SP_JsonObjectNode * SP_HiveRespObjectGatherJson :: getErrdata()
{
	return mErrdata;
}

