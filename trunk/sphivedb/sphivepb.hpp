/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#ifndef __sphivepb_hpp__
#define __sphivepb_hpp__

#include "sphivemsg.hpp"

class SP_ProtoBufRpcReqObject;
class SP_ProtoBufRpcRespObject;

class SP_ProtoBufTree;

class SP_HiveReqObjectProtoBuf : public SP_HiveReqObject {
public:

	enum {
		eDBFile = 1,
		eUser = 2,
		eDBName = 3,
		eSQL = 4
	};

public:
	SP_HiveReqObjectProtoBuf( SP_ProtoBufRpcReqObject * inner );
	~SP_HiveReqObjectProtoBuf();

	const char * verify();

	const char * verifyWithoutSql();

	int getDBFile();

	const char * getUser();

	const char * getDBName();

	int getSqlCount();

	const char * getSql( int index );

	const char * getUniqKey();

private:
	SP_ProtoBufRpcReqObject * mInner;

	char mUniqKey[ 128 ];
};

class SP_HiveRespObjectProtoBuf : public SP_HiveRespObject {
public:

	enum {
		eFakeFN = 1,

		eName = 1,
		eType = 2,
		eRow = 3
	};

public:
	SP_HiveRespObjectProtoBuf( SP_ProtoBufRpcRespObject * inner, int toBeOwner = 0 );
	~SP_HiveRespObjectProtoBuf();

	int getErrorCode();
	const char * getErrorMsg();

	int getErrdataCode();
	const char * getErrdataMsg();

	int getResultCount();

	/* caller must delete the return value */
	SP_HiveResultSet * getResultSet( int index );

private:
	SP_ProtoBufRpcRespObject * mInner;
	int mToBeOwner;
};

class SP_HiveResultSetProtoBuf : public SP_HiveResultSet {
public:
	SP_HiveResultSetProtoBuf( SP_ProtoBufTree * typeList,
			SP_ProtoBufTree * nameList, SP_ProtoBufTree * rowList );
	~SP_HiveResultSetProtoBuf();

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
	SP_ProtoBufTree * mTypeList;
	SP_ProtoBufTree * mNameList;
	SP_ProtoBufTree * mRowList;

	int mRowIndex;
};

#endif

