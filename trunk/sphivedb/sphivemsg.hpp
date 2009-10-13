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
	virtual ~SP_HiveReqObject();

	virtual const char * verify() = 0;

	virtual const char * verifyWithoutSql() = 0;

	virtual int getDBFile() = 0;

	virtual const char * getUser() = 0;

	virtual const char * getDBName() = 0;

	virtual int getSqlCount() = 0;

	virtual const char * getSql( int index ) = 0;

	virtual const char * getUniqKey() = 0;
};

class SP_HiveRespObject {
public:
	virtual ~SP_HiveRespObject();

	virtual int getErrorCode() = 0;
	virtual const char * getErrorMsg() = 0;

	virtual int getErrdataCode() = 0;
	virtual const char * getErrdataMsg() = 0;

	virtual int getResultCount() = 0;

	/* caller must delete the return value */
	virtual SP_HiveResultSet * getResultSet( int index ) = 0;
};

//====================================================================

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

#endif

