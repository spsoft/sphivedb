/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#ifndef __sphivegather_hpp__
#define __sphivegather_hpp__

class SP_JsonObjectNode;
class SP_JsonArrayNode;

class SP_HiveResultSetGather {
public:
	virtual ~SP_HiveResultSetGather();

	virtual int addType( const char * type ) = 0;

	virtual int addName( const char * name ) = 0;

	virtual int addRow() = 0;

	virtual int addColumn( const char * value ) = 0;

	virtual int addColumn( int value ) = 0;
};

class SP_HiveRespObjectGather {
public:
	virtual ~SP_HiveRespObjectGather();

	virtual int addResultSet() = 0;

	virtual SP_HiveResultSetGather * getResultSet() = 0;

	virtual int reportError( int errcode, const char * errmsg ) = 0;
};

//====================================================================

class SP_HiveResultSetGatherJson : public SP_HiveResultSetGather {
public:
	SP_HiveResultSetGatherJson( SP_JsonObjectNode * inner );

	virtual ~SP_HiveResultSetGatherJson();

	virtual int addType( const char * type );

	virtual int addName( const char * name );

	virtual int addRow();

	virtual int addColumn( const char * value );

	virtual int addColumn( int value );

private:
	SP_JsonObjectNode * mInner;

	SP_JsonArrayNode * mTypeList;
	SP_JsonArrayNode * mNameList;

	SP_JsonArrayNode * mRowList;

	SP_JsonArrayNode * mRow;
};

class SP_HiveRespObjectGatherJson : public SP_HiveRespObjectGather {
public:
	SP_HiveRespObjectGatherJson();
	virtual ~SP_HiveRespObjectGatherJson();

	virtual int addResultSet();

	virtual SP_HiveResultSetGather * getResultSet();

	virtual int reportError( int errcode, const char * errmsg );

	SP_JsonArrayNode * getResult();

	SP_JsonObjectNode * getErrdata();

private:
	SP_JsonArrayNode * mResult;
	SP_JsonObjectNode * mErrdata;

	SP_HiveResultSetGather * mResultSet;
};

#endif

