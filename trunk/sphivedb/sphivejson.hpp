/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#ifndef __sphivejson_hpp__
#define __sphivejson_hpp__

#include "sphivemsg.hpp"

class SP_JsonRpcReqObject;
class SP_JsonRpcRespObject;

class SP_JsonObjectNode;
class SP_JsonArrayNode;

class SP_HiveReqObjectJson : public SP_HiveReqObject {
public:
	SP_HiveReqObjectJson( SP_JsonRpcReqObject * inner );
	~SP_HiveReqObjectJson();

	const char * verify();

	const char * verifyWithoutSql();

	int getDBFile();

	const char * getUser();

	const char * getDBName();

	int getSqlCount();

	const char * getSql( int index );

	const char * getUniqKey();

private:
	SP_JsonRpcReqObject * mInner;
	char mUniqKey[ 128 ];
};

class SP_HiveRespObjectJson : public SP_HiveRespObject {
public:
	SP_HiveRespObjectJson( SP_JsonRpcRespObject * inner, int toBeOwner = 0 );
	~SP_HiveRespObjectJson();

	int getErrorCode();
	const char * getErrorMsg();

	int getErrdataCode();
	const char * getErrdataMsg();

	int getResultCount();

	/* caller must delete the return value */
	SP_HiveResultSet * getResultSet( int index );

private:
	SP_JsonRpcRespObject * mInner;
	int mToBeOwner;
};

class SP_HiveResultSetJson : public SP_HiveResultSet {
public:
	SP_HiveResultSetJson( const SP_JsonObjectNode * inner );
	~SP_HiveResultSetJson();

	int getColumnCount();

	const char * getType( int index );
	const char * getName( int index );

	int getRowCount();

	int moveTo( int index );

	const char * getString( int index );
	int getInt( int index );
	double getDouble( int index );

	const char * getAsString( int index, char * buffer, int len );

private:
	const SP_JsonObjectNode * mInner;

	const SP_JsonArrayNode * mTypeList;
	const SP_JsonArrayNode * mNameList;
	const SP_JsonArrayNode * mRowList;

	int mRowIndex;
};

#endif

