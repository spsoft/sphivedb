/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#include <stdio.h>

#include "sphivepb.hpp"

#include "spjson/sppbtree.hpp"
#include "spjson/sppbrpc.hpp"

SP_HiveReqObjectProtoBuf :: SP_HiveReqObjectProtoBuf( SP_ProtoBufRpcReqObject * inner )
{
	mInner = inner;
	mUniqKey[0] = '\0';
}

SP_HiveReqObjectProtoBuf :: ~SP_HiveReqObjectProtoBuf()
{
}

const char * SP_HiveReqObjectProtoBuf :: verify()
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

const char * SP_HiveReqObjectProtoBuf :: verifyWithoutSql()
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

int SP_HiveReqObjectProtoBuf :: getDBFile()
{
	SP_ProtoBufDecoder::KeyValPair_t pair;

	SP_ProtoBufTree * params = mInner->getParams();

	if( NULL != params ) {
		if( params->getDecoder()->find( eDBFile, &pair ) ) {
			return pair.m32Bit.s;
		}
	}

	return -1;
}

const char * SP_HiveReqObjectProtoBuf :: getUser()
{
	SP_ProtoBufDecoder::KeyValPair_t pair;

	SP_ProtoBufTree * params = mInner->getParams();

	if( NULL != params ) {
		if( params->getDecoder()->find( eUser, &pair ) ) {
			return pair.mBinary.mBuffer;
		}
	}

	return NULL;
}

const char * SP_HiveReqObjectProtoBuf :: getDBName()
{
	SP_ProtoBufDecoder::KeyValPair_t pair;

	SP_ProtoBufTree * params = mInner->getParams();

	if( NULL != params ) {
		if( params->getDecoder()->find( eDBName, &pair ) ) {
			return pair.mBinary.mBuffer;
		}
	}

	return NULL;
}

int SP_HiveReqObjectProtoBuf :: getSqlCount()
{
	SP_ProtoBufDecoder::KeyValPair_t pair;

	SP_ProtoBufTree * params = mInner->getParams();

	if( NULL != params ) {
		if( params->getDecoder()->find( eSQL, &pair ) ) {
			return pair.mRepeatedCount;
		}
	}

	return 0;
}

const char * SP_HiveReqObjectProtoBuf :: getSql( int index )
{
	SP_ProtoBufDecoder::KeyValPair_t pair;

	SP_ProtoBufTree * params = mInner->getParams();

	if( NULL != params ) {
		if( params->getDecoder()->find( eSQL, &pair, index ) ) {
			return pair.mBinary.mBuffer;
		}
	}

	return 0;
}

const char * SP_HiveReqObjectProtoBuf :: getUniqKey()
{
	if( '\0' == mUniqKey[0] ) {
		snprintf( mUniqKey, sizeof( mUniqKey ), "%d/%s/%s", getDBFile(), getUser(), getDBName() );
	}

	return mUniqKey;
}

//====================================================================

SP_HiveRespObjectProtoBuf :: SP_HiveRespObjectProtoBuf( SP_ProtoBufRpcRespObject * inner, int toBeOwner )
{
	mInner = inner;
	mToBeOwner = toBeOwner;
}

SP_HiveRespObjectProtoBuf :: ~SP_HiveRespObjectProtoBuf()
{
	if( mToBeOwner ) delete mInner, mInner = NULL;
}

int SP_HiveRespObjectProtoBuf :: getErrorCode()
{
	return mInner->getErrorCode();
}

const char * SP_HiveRespObjectProtoBuf :: getErrorMsg()
{
	return mInner->getErrorMsg();
}

int SP_HiveRespObjectProtoBuf :: getErrdataCode()
{
	SP_ProtoBufDecoder::KeyValPair_t pair;

	SP_ProtoBufTree * error = mInner->getError();
	if( NULL != error ) {
		SP_ProtoBufTree * errdata = error->findChild( SP_ProtoBufRpcRespObject::eErrorData );
		if( NULL != errdata ) {
			if( errdata->getDecoder()->find( SP_ProtoBufRpcRespObject::eErrorCode, &pair ) ) {
				return pair.m32Bit.s;
			}
		}
	}

	return 0;
}

const char * SP_HiveRespObjectProtoBuf :: getErrdataMsg()
{
	SP_ProtoBufDecoder::KeyValPair_t pair;

	SP_ProtoBufTree * error = mInner->getError();
	if( NULL != error ) {
		SP_ProtoBufTree * errdata = error->findChild( SP_ProtoBufRpcRespObject::eErrorData );
		if( NULL != errdata ) {
			if( errdata->getDecoder()->find( SP_ProtoBufRpcRespObject::eErrorMsg, &pair ) ) {
				return pair.mBinary.mBuffer;
			}
		}
	}

	return 0;
}

int SP_HiveRespObjectProtoBuf :: getResultCount()
{
	SP_ProtoBufDecoder::KeyValPair_t pair;

	SP_ProtoBufTree * result = mInner->getResult();

	if( NULL != result ) {
		if( result->getDecoder()->find( eFakeFN, &pair ) ) {
			return pair.mRepeatedCount;
		}
	}

	return 0;
}

/* caller must delete the return value */
SP_HiveResultSet * SP_HiveRespObjectProtoBuf :: getResultSet( int index )
{
	SP_ProtoBufTree * result = mInner->getResult();

	if( NULL != result ) {
		SP_ProtoBufTree * typeList = result->findChild( SP_HiveRespObjectProtoBuf::eType, index );
		SP_ProtoBufTree * nameList = result->findChild( SP_HiveRespObjectProtoBuf::eName, index );
		SP_ProtoBufTree * rowList = result->findChild( SP_HiveRespObjectProtoBuf::eRow, index );

		if( NULL != typeList && NULL != nameList && NULL != rowList ) {
			return new SP_HiveResultSetProtoBuf( typeList, nameList, rowList );
		}
	}

	return NULL;
}

//====================================================================

SP_HiveResultSetProtoBuf :: SP_HiveResultSetProtoBuf( SP_ProtoBufTree * typeList,
			SP_ProtoBufTree * nameList, SP_ProtoBufTree * rowList )
{
	mTypeList = typeList;
	mNameList = nameList;
	mRowList = rowList;

	mRowIndex = 0;
}

SP_HiveResultSetProtoBuf :: ~SP_HiveResultSetProtoBuf()
{
}

int SP_HiveResultSetProtoBuf :: getColumnCount()
{
	SP_ProtoBufDecoder::KeyValPair_t pair;

	if( mTypeList->getDecoder()->find( SP_HiveRespObjectProtoBuf::eFakeFN, &pair ) ) {
		return pair.mRepeatedCount;
	}

	return 0;
}

const char * SP_HiveResultSetProtoBuf :: getType( int index )
{
	SP_ProtoBufDecoder::KeyValPair_t pair;

	if( mTypeList->getDecoder()->find( SP_HiveRespObjectProtoBuf::eFakeFN, &pair, index ) ) {
		return pair.mBinary.mBuffer;
	}

	return 0;
}

const char * SP_HiveResultSetProtoBuf :: getName( int index )
{
	SP_ProtoBufDecoder::KeyValPair_t pair;

	if( mNameList->getDecoder()->find( SP_HiveRespObjectProtoBuf::eFakeFN, &pair, index ) ) {
		return pair.mBinary.mBuffer;
	}

	return 0;
}

int SP_HiveResultSetProtoBuf :: getRowCount()
{
	SP_ProtoBufDecoder::KeyValPair_t pair;

	if( mRowList->getDecoder()->find( SP_HiveRespObjectProtoBuf::eFakeFN, &pair ) ) {
		return pair.mRepeatedCount;
	}

	return 0;
}

int SP_HiveResultSetProtoBuf :: moveTo( int index )
{
	mRowIndex = index;

	return 0;
}

const char * SP_HiveResultSetProtoBuf :: getString( int index )
{
	SP_ProtoBufDecoder::KeyValPair_t pair;

	SP_ProtoBufTree * tree = mRowList->findChild( SP_HiveRespObjectProtoBuf::eFakeFN, mRowIndex );

	if( NULL != tree ) {
		if( tree->getDecoder()->find( SP_HiveRespObjectProtoBuf::eFakeFN, &pair, index ) ) {
			return pair.mBinary.mBuffer;
		}
	}

	return NULL;
}

int SP_HiveResultSetProtoBuf :: getInt( int index )
{
	SP_ProtoBufDecoder::KeyValPair_t pair;

	SP_ProtoBufTree * tree = mRowList->findChild( SP_HiveRespObjectProtoBuf::eFakeFN, mRowIndex );

	if( NULL != tree ) {
		if( tree->getDecoder()->find( SP_HiveRespObjectProtoBuf::eFakeFN, &pair, index ) ) {
			return pair.m32Bit.s;
		}
	}

	return 0;
}

double SP_HiveResultSetProtoBuf :: getDouble( int index )
{
	SP_ProtoBufDecoder::KeyValPair_t pair;

	SP_ProtoBufTree * tree = mRowList->findChild( SP_HiveRespObjectProtoBuf::eFakeFN, mRowIndex );

	if( NULL != tree ) {
		if( tree->getDecoder()->find( SP_HiveRespObjectProtoBuf::eFakeFN, &pair, index ) ) {
			return pair.m64Bit.d;
		}
	}

	return 0;
}

const char * SP_HiveResultSetProtoBuf :: getAsString( int index, char * buffer, int len )
{
	SP_ProtoBufDecoder::KeyValPair_t pair;

	SP_ProtoBufTree * tree = mRowList->findChild( SP_HiveRespObjectProtoBuf::eFakeFN, mRowIndex );

	if( NULL != tree ) {
		if( tree->getDecoder()->find( SP_HiveRespObjectProtoBuf::eFakeFN, &pair, index ) ) {
			if( SP_ProtoBufCodecUtils::toString( &pair, buffer, len ) ) return buffer;
		}
	}

	return NULL;
}

