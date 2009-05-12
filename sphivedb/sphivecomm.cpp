/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#include <stdio.h>

#include "sphivecomm.hpp"

#include "spjson/spjsonnode.hpp"
#include "spjson/spjsonhandle.hpp"
#include "spjson/spjsondomparser.hpp"

SP_HiveResultSet :: SP_HiveResultSet( const SP_JsonObjectNode * inner )
{
	mInner = inner;

	SP_JsonHandle handle( mInner );

	mTypeList = handle.getChild( "type" ).toArray();
	mNameList = handle.getChild( "name" ).toArray();
	mRowList = handle.getChild( "row" ).toArray();

	mRowIndex = 0;
}

SP_HiveResultSet :: ~SP_HiveResultSet()
{
}

int SP_HiveResultSet :: getColumnCount()
{
	return NULL != mTypeList ? mTypeList->getCount() : 0 ;
}

const char * SP_HiveResultSet :: getType( int index )
{
	SP_JsonHandle handle( mTypeList );

	SP_JsonStringNode * node = handle.getChild( index ).toString();

	return NULL != node ? node->getValue() : NULL;
}

const char * SP_HiveResultSet :: getName( int index )
{
	SP_JsonHandle handle( mNameList );

	SP_JsonStringNode * node = handle.getChild( index ).toString();

	return NULL != node ? node->getValue() : NULL;
}

int SP_HiveResultSet :: getRowCount()
{
	return NULL != mRowList ? mRowList->getCount() : 0;
}

int SP_HiveResultSet :: moveTo( int index )
{
	mRowIndex = index;

	return 0;
}

const char * SP_HiveResultSet :: getString( int index )
{
	SP_JsonHandle handle( mRowList );

	SP_JsonStringNode * node = handle.getChild( mRowIndex ).getChild( index ).toString();

	return NULL != node ? node->getValue() : NULL;
}

int SP_HiveResultSet :: getInt( int index )
{
	SP_JsonHandle handle( mRowList );

	SP_JsonIntNode * node = handle.getChild( mRowIndex ).getChild( index ).toInt();

	return NULL != node ? node->getValue() : 0;
}

double SP_HiveResultSet :: getDouble( int index )
{
	SP_JsonHandle handle( mRowList );

	SP_JsonDoubleNode * node = handle.getChild( mRowIndex ).getChild( index ).toDouble();

	return NULL != node ? node->getValue() : 0;
}

const char * SP_HiveResultSet :: getAsString( int index, char * buffer, int len )
{
	SP_JsonHandle handle( mRowList );

	SP_JsonNode * node = handle.getChild( mRowIndex ).getChild( index ).toNode();

	const char * ret = NULL;

	if( NULL != node ) {
		if( node->getType() == SP_JsonNode::eString ) {
			ret = ((SP_JsonStringNode*)node)->getValue();
		} else {
			SP_JsonDomBuffer::dump( node, buffer, len );
			ret = buffer;
		}
	}

	return ret;
}

