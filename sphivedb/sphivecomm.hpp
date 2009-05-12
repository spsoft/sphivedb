/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#ifndef __sphivecomm_hpp__
#define __sphivecomm_hpp__

class SP_JsonObjectNode;
class SP_JsonArrayNode;

class SP_HiveResultSet {
public:
	SP_HiveResultSet( const SP_JsonObjectNode * inner );
	~SP_HiveResultSet();

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

