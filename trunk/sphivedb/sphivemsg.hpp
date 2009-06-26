/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#ifndef __sphivejson_hpp__
#define __sphivejson_hpp__

class SP_JsonRpcReqObject;
class SP_JsonRpcRespObject;
class SP_HiveResultSet;

class SP_HiveReqObject {
public:
	SP_HiveReqObject( SP_JsonRpcReqObject * inner );
	~SP_HiveReqObject();

	const char * verify();

	int getDBFile();

	const char * getUser();

	const char * getDBName();

	int getSqlCount();

	const char * getSql( int index );

private:
	SP_JsonRpcReqObject * mInner;
};

class SP_HiveRespObject {
public:
	SP_HiveRespObject( SP_JsonRpcRespObject * inner, int toBeOwner = 0 );
	~SP_HiveRespObject();

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

#endif

