/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#ifndef __sphivemsg_hpp__
#define __sphivemsg_hpp__

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

class SP_HiveResultSet {
public:
	virtual ~SP_HiveResultSet();

	virtual int getColumnCount() = 0;

	virtual const char * getType( int index ) = 0;
	virtual const char * getName( int index ) = 0;

	virtual int getRowCount() = 0;

	virtual int moveTo( int index ) = 0;

	virtual const char * getString( int index ) = 0;
	virtual int getInt( int index ) = 0;
	virtual double getDouble( int index ) = 0;

	virtual const char * getAsString( int index, char * buffer, int len ) = 0;
};

#endif

