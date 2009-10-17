/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#ifndef __sphivegather_hpp__
#define __sphivegather_hpp__

class SP_JsonObjectNode;
class SP_JsonArrayNode;

class SP_ProtoBufEncoder;

class SP_HiveResultSetGather {
public:
	virtual ~SP_HiveResultSetGather();

	virtual int addType( const char * type ) = 0;

	virtual int addName( const char * name ) = 0;

	virtual int addColumn( const char * value ) = 0;

	virtual int addColumn( int value ) = 0;

	virtual int submitRow() = 0;
};

class SP_HiveRespObjectGather {
public:
	virtual ~SP_HiveRespObjectGather();

	virtual SP_HiveResultSetGather * getResultSet() = 0;

	virtual int submitResultSet() = 0;

	virtual int reportError( int errcode, const char * errmsg ) = 0;

	virtual int reportErrdata( int errcode, const char * errmsg ) = 0;
};

//====================================================================

class SP_HiveResultSetGatherJson : public SP_HiveResultSetGather {
public:
	SP_HiveResultSetGatherJson();

	virtual ~SP_HiveResultSetGatherJson();

	virtual int addType( const char * type );

	virtual int addName( const char * name );

	virtual int addColumn( const char * value );

	virtual int addColumn( int value );

	virtual int submitRow();

	virtual int submit( SP_JsonObjectNode * result );

private:
	void initInner();

private:
	SP_JsonArrayNode * mTypeList;
	SP_JsonArrayNode * mNameList;

	SP_JsonArrayNode * mRowList;

	SP_JsonArrayNode * mRow;
};

class SP_HiveRespObjectGatherJson : public SP_HiveRespObjectGather {
public:
	SP_HiveRespObjectGatherJson();
	virtual ~SP_HiveRespObjectGatherJson();

	virtual SP_HiveResultSetGather * getResultSet();

	virtual int submitResultSet();

	virtual int reportError( int errcode, const char * errmsg );

	virtual int reportErrdata( int errcode, const char * errmsg );

	SP_JsonArrayNode * getResult();

	SP_JsonObjectNode * getError();

private:
	SP_JsonArrayNode * mResult;
	SP_JsonObjectNode * mError;
	SP_JsonObjectNode * mErrdata;

	SP_HiveResultSetGatherJson * mResultSet;
};

//====================================================================

class SP_HiveResultSetGatherProtoBuf : public SP_HiveResultSetGather {
public:
	SP_HiveResultSetGatherProtoBuf();

	virtual ~SP_HiveResultSetGatherProtoBuf();

	virtual int addType( const char * type );

	virtual int addName( const char * name );

	virtual int addColumn( const char * value );

	virtual int addColumn( int value );

	virtual int submitRow();

	virtual int submit( SP_ProtoBufEncoder * result );

private:
	SP_ProtoBufEncoder * mTypeList;
	SP_ProtoBufEncoder * mNameList;

	SP_ProtoBufEncoder * mRowList;

	SP_ProtoBufEncoder * mRow;
};

class SP_HiveRespObjectGatherProtoBuf : public SP_HiveRespObjectGather {
public:
	SP_HiveRespObjectGatherProtoBuf();
	virtual ~SP_HiveRespObjectGatherProtoBuf();

	virtual SP_HiveResultSetGather * getResultSet();

	virtual int submitResultSet();

	virtual int reportError( int errcode, const char * errmsg );

	virtual int reportErrdata( int errcode, const char * errmsg );

	SP_ProtoBufEncoder * getResult();

	SP_ProtoBufEncoder * getError();

private:
	SP_ProtoBufEncoder * mResult;
	SP_ProtoBufEncoder * mError;

	SP_HiveResultSetGatherProtoBuf * mResultSet;
};

#endif

