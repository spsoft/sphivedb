/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#include <stdio.h>

#include "sphivegather.hpp"
#include "sphivepb.hpp"

#include "spjson/spjsonnode.hpp"
#include "spjson/spjsonrpc.hpp"
#include "spjson/sppbcodec.hpp"
#include "spjson/sppbrpc.hpp"

SP_HiveResultSetGather :: ~SP_HiveResultSetGather()
{
}

SP_HiveRespObjectGather :: ~SP_HiveRespObjectGather()
{
}

//====================================================================

SP_HiveResultSetGatherJson :: SP_HiveResultSetGatherJson()
{
	mNameList = NULL;
	mTypeList = NULL;
	mRowList = NULL;

	mRow = NULL;
}

SP_HiveResultSetGatherJson :: ~SP_HiveResultSetGatherJson()
{
	if( NULL != mNameList ) delete mNameList, mNameList = NULL;
	if( NULL != mTypeList ) delete mTypeList, mTypeList = NULL;
	if( NULL != mRowList ) delete mRowList, mRowList = NULL;
	if( NULL != mRow ) delete mRow, mRow = NULL;
}

int SP_HiveResultSetGatherJson :: addType( const char * type )
{
	if( NULL == mTypeList ) mTypeList = new SP_JsonArrayNode();

	mTypeList->addValue( new SP_JsonStringNode( type ) );

	return 0;
}

int SP_HiveResultSetGatherJson :: addName( const char * name )
{
	if( NULL == mNameList ) mNameList = new SP_JsonArrayNode();

	mNameList->addValue( new SP_JsonStringNode( name ) );

	return 0;
}

int SP_HiveResultSetGatherJson :: addColumn( const char * value )
{
	if( NULL == mRow ) mRow = new SP_JsonArrayNode();

	if( NULL != value ) {
		mRow->addValue( new SP_JsonStringNode( value ) );
	} else {
		mRow->addValue( new SP_JsonNullNode() );
	}

	return 0;
}

int SP_HiveResultSetGatherJson :: addColumn( int value )
{
	if( NULL == mRow ) mRow = new SP_JsonArrayNode();

	mRow->addValue( new SP_JsonIntNode( value ) );

	return 0;
}

int SP_HiveResultSetGatherJson :: submitRow()
{
	if( NULL == mRowList ) mRowList = new SP_JsonArrayNode();

	if( NULL != mRow ) {
		mRowList->addValue( mRow );
		mRow = NULL;
		return 0;
	}

	return -1;
}

int SP_HiveResultSetGatherJson :: submit( SP_JsonObjectNode * result )
{
	if( NULL == mNameList || NULL == mTypeList || NULL == mRowList ) return -1;

	{
		SP_JsonPairNode * pair = new SP_JsonPairNode();

		pair->setName( "name" );
		pair->setValue( mNameList );

		result->addValue( pair );

		mNameList = NULL;
	}

	{
		SP_JsonPairNode * pair = new SP_JsonPairNode();

		pair->setName( "type" );
		pair->setValue( mTypeList );

		result->addValue( pair );

		mTypeList = NULL;
	}

	{
		SP_JsonPairNode * pair = new SP_JsonPairNode();

		pair->setName( "row" );
		pair->setValue( mRowList );

		result->addValue( pair );

		mRowList = NULL;
	}

	return 0;
}

//====================================================================

SP_HiveRespObjectGatherJson :: SP_HiveRespObjectGatherJson()
{
	mResult = new SP_JsonArrayNode();

	mError = new SP_JsonObjectNode();
	{
		mErrdata = new SP_JsonObjectNode();

		SP_JsonPairNode * dataPair = new SP_JsonPairNode();
		dataPair->setName( "data" );
		dataPair->setValue( mErrdata );
		mError->addValue( dataPair );
	}

	mResultSet = NULL;
}

SP_HiveRespObjectGatherJson :: ~SP_HiveRespObjectGatherJson()
{
	delete mResult;
	delete mError;

	if( NULL != mResultSet ) delete mResultSet;
}

SP_HiveResultSetGather * SP_HiveRespObjectGatherJson :: getResultSet()
{
	if( NULL == mResultSet ) mResultSet = new SP_HiveResultSetGatherJson();

	return mResultSet;
}

int SP_HiveRespObjectGatherJson :: submitResultSet()
{
	SP_JsonObjectNode * inner = new SP_JsonObjectNode();
	mResult->addValue( inner );

	return mResultSet->submit( inner );
}

int SP_HiveRespObjectGatherJson :: reportError( int errcode, const char * errmsg )
{
	SP_JsonRpcUtils::setError( mError, errcode, errmsg );

	return 0;
}

int SP_HiveRespObjectGatherJson :: reportErrdata( int errcode, const char * errmsg )
{
	SP_JsonRpcUtils::setError( mErrdata, errcode, errmsg );

	return 0;
}

SP_JsonArrayNode * SP_HiveRespObjectGatherJson :: getResult()
{
	return mResult;
}

SP_JsonObjectNode * SP_HiveRespObjectGatherJson :: getError()
{
	return mError;
}

//====================================================================

SP_HiveResultSetGatherProtoBuf :: SP_HiveResultSetGatherProtoBuf()
{
	mTypeList = new SP_ProtoBufEncoder();
	mNameList = new SP_ProtoBufEncoder();

	mRowList = new SP_ProtoBufEncoder();

	mRow = new SP_ProtoBufEncoder();
}

SP_HiveResultSetGatherProtoBuf :: ~SP_HiveResultSetGatherProtoBuf()
{
	delete mTypeList, mTypeList = NULL;
	delete mNameList, mNameList = NULL;

	delete mRowList, mRowList = NULL;

	delete mRow, mRow = NULL;
}

int SP_HiveResultSetGatherProtoBuf :: addType( const char * type )
{
	return mTypeList->addString( SP_HiveRespObjectProtoBuf::eFakeFN, type );
}

int SP_HiveResultSetGatherProtoBuf :: addName( const char * name )
{
	return mNameList->addString( SP_HiveRespObjectProtoBuf::eFakeFN, name );
}

int SP_HiveResultSetGatherProtoBuf :: addColumn( const char * value )
{
	return mRow->addString(  SP_HiveRespObjectProtoBuf::eFakeFN, value ? value : "" );
}

int SP_HiveResultSetGatherProtoBuf :: addColumn( int value )
{
	return mRow->add32Bit( SP_HiveRespObjectProtoBuf::eFakeFN, value );
}

int SP_HiveResultSetGatherProtoBuf :: submitRow()
{
	mRowList->addBinary( SP_HiveRespObjectProtoBuf::eFakeFN,
			mRow->getBuffer(), mRow->getSize() );
	mRow->reset();

	return 0;
}

int SP_HiveResultSetGatherProtoBuf :: submit( SP_ProtoBufEncoder * result )
{
	result->addBinary( SP_HiveRespObjectProtoBuf::eName,
			mNameList->getBuffer(), mNameList->getSize() );

	result->addBinary( SP_HiveRespObjectProtoBuf::eType,
			mTypeList->getBuffer(), mTypeList->getSize() );

	result->addBinary( SP_HiveRespObjectProtoBuf::eRow,
			mRowList->getBuffer(), mRowList->getSize() );

	mNameList->reset();
	mTypeList->reset();
	mRowList->reset();

	return 0;
}

//====================================================================

SP_HiveRespObjectGatherProtoBuf :: SP_HiveRespObjectGatherProtoBuf()
{
	mResult = new SP_ProtoBufEncoder();
	mError = new SP_ProtoBufEncoder();

	mResultSet = new SP_HiveResultSetGatherProtoBuf();
}

SP_HiveRespObjectGatherProtoBuf :: ~SP_HiveRespObjectGatherProtoBuf()
{
	delete mResult, mResult = NULL;
	delete mError, mError = NULL;

	delete mResultSet, mResultSet = NULL;
}

int SP_HiveRespObjectGatherProtoBuf :: submitResultSet()
{
	return mResultSet->submit( mResult );
}

SP_HiveResultSetGather * SP_HiveRespObjectGatherProtoBuf :: getResultSet()
{
	return mResultSet;
}

int SP_HiveRespObjectGatherProtoBuf :: reportError( int errcode, const char * errmsg )
{
	mError->add32Bit( SP_ProtoBufRpcRespObject::eErrorCode, errcode );
	mError->addString( SP_ProtoBufRpcRespObject::eErrorMsg, errmsg );

	return 0;
}

int SP_HiveRespObjectGatherProtoBuf :: reportErrdata( int errcode, const char * errmsg )
{
	SP_ProtoBufEncoder errdata;

	errdata.add32Bit( SP_ProtoBufRpcRespObject::eErrorCode, errcode );
	errdata.addString( SP_ProtoBufRpcRespObject::eErrorMsg, errmsg );

	mError->addBinary( SP_ProtoBufRpcRespObject::eErrorData, errdata.getBuffer(), errdata.getSize() );

	return 0;
}

SP_ProtoBufEncoder * SP_HiveRespObjectGatherProtoBuf :: getResult()
{
	return mResult;
}

SP_ProtoBufEncoder * SP_HiveRespObjectGatherProtoBuf :: getError()
{
	return mError;
}

