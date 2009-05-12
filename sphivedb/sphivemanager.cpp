/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#include <stdio.h>
#include <string.h>

#include "sphivemanager.hpp"
#include "spmemvfs.h"
#include "sphivemsg.hpp"

#include "tcadb.h" // tokyocabinet

#include "sqlite3.h"

#include "spnetkit/spnklog.hpp"
#include "spnetkit/spnkporting.hpp"

#include "spjson/spjsonnode.hpp"

void * SP_HiveManager :: load( void * arg, const char * path, int * len )
{
	spmembuffer_map_t * themap = (spmembuffer_map_t*)arg;

	void * ret = spmembuffer_map_take( themap, path, len );

	return ret;
}

int SP_HiveManager :: save( void * arg, const char * path, char * buffer, int len )
{
	spmembuffer_map_t * themap = (spmembuffer_map_t*)arg;

	return spmembuffer_map_put( themap, path, buffer, len );
}

SP_HiveManager :: SP_HiveManager()
{
	mDbm = NULL;
	mBuffMap = NULL;
}

SP_HiveManager :: ~SP_HiveManager()
{
	if( NULL != mDbm ) tcadbdel( (TCADB*) mDbm );
	mDbm = NULL;

	if( NULL != mBuffMap ) spmembuffer_map_del( mBuffMap );
	mBuffMap = NULL;
}

int SP_HiveManager :: init( const char * datadir )
{
	char name[ 256 ] = { 0 };

	snprintf( name, sizeof( name ), "%s/sphive0.tch", datadir );

	TCADB * adb = tcadbnew();

	if( tcadbopen( adb, name ) ) {
		mDbm = adb;
	} else {
		tcadbdel( adb );
	}

	if( NULL != mDbm ) {
		mBuffMap = spmembuffer_map_new();
		spmemvfs_cb_t cb = { mBuffMap, load, save };
		spmemvfs_init( &cb );
	}

	return NULL != mDbm ? 0 : -1;
}

int SP_HiveManager :: execute( SP_JsonRpcReqObject * rpcReq, SP_JsonArrayNode * result )
{
	int ret = 0;

	TCADB * adb = (TCADB*)mDbm;

	SP_HiveReqObject reqObject( rpcReq );
	const char * path = reqObject.getPath();

	// TODO: lock the path

	// read block from dbm
	{
		int vlen = 0;
		void * vbuf = tcadbget( adb, path, strlen( path ), &vlen );

		if( NULL != vbuf ) {
			spmembuffer_map_put( mBuffMap, path, vbuf, vlen );
		}
	}

	int hasUpdate = 0;

	// init sqlite3, execute sql script
	{
		sqlite3 * dbHandle = NULL;

		int dbRet = sqlite3_open_v2( path, &dbHandle,
				SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, SPMEMVFS_NAME );

		for( int i = 0; i < reqObject.getSqlCount(); i++ ) {
			const char * sql = reqObject.getSql( i );

			if( 0 == strncasecmp( "select", sql, 6 ) ) {
				dbRet = doSelect( dbHandle, sql, result );
			} else {
				dbRet = doUpdate( dbHandle, sql, result );
				if( dbRet > 0 ) hasUpdate = 1;
			}

			if( dbRet < 0 ) {
				ret = -1;
				break;
			}
		}

		dbRet = sqlite3_close( dbHandle );
	}

	// write block to dbm
	{
		int vlen = 0;
		char * vbuf = (char*)spmembuffer_map_take( mBuffMap, path, &vlen );

		if( NULL != vbuf ) {
			if( 0 == ret && hasUpdate ) {
				if( ! tcadbput( adb, path, strlen( path ), vbuf, vlen ) ) {
					SP_NKLog::log( LOG_ERR, "ERROR: tcadbput fail" );
				}
			}
			free( vbuf );
		}
	}

	return ret;
}

int SP_HiveManager :: doSelect( sqlite3 * handle, const char * sql, SP_JsonArrayNode * result )
{
	sqlite3_stmt * stmt = NULL;

	int dbRet = sqlite3_prepare( handle, sql, strlen( sql ), &stmt, NULL );

	if( SQLITE_OK != dbRet ) {
		SP_NKLog::log( LOG_ERR, "ERROR: sqlite3_prepare(%s) = %d", sql, dbRet );

		if( NULL != stmt ) sqlite3_finalize( stmt );
		return -1;
	}

	int count = sqlite3_column_count( stmt );

	SP_JsonObjectNode * rs = new SP_JsonObjectNode();

	SP_JsonPairNode * rowPair = new SP_JsonPairNode();
	{
		SP_JsonArrayNode * rowList = new SP_JsonArrayNode();

		for( ; ; ) {
			int stepRet = sqlite3_step( stmt );

			if( SQLITE_DONE != stepRet && SQLITE_ROW != stepRet ) {
				dbRet = -1;
				SP_NKLog::log( LOG_ERR, "ERROR: sqlite3_step(%s) = %d", sql, stepRet );
				break;
			}

			if( SQLITE_DONE == stepRet ) break;

			SP_JsonArrayNode * row = new SP_JsonArrayNode();

			for( int i = 0; i < count; i++ ) {
				const char * value = (char*)sqlite3_column_text( stmt, i );

				SP_JsonStringNode * column = new SP_JsonStringNode( value );
				row->addValue( column );
			}
			rowList->addValue( row );
		}


		rowPair->setName( "row" );
		rowPair->setValue( rowList );

		rs->addValue( rowPair );
	}

	SP_JsonPairNode * typePair = new SP_JsonPairNode();
	{
		SP_JsonArrayNode * type = new SP_JsonArrayNode();

		for( int i = 0; i < count; i++ ) {
			const char * value = sqlite3_column_decltype( stmt, i );
			value = value ? value : "null";

			type->addValue( new SP_JsonStringNode( value ) );
		}

		typePair->setName( "type" );
		typePair->setValue( type );

		rs->addValue( typePair );
	}

	SP_JsonPairNode * namePair = new SP_JsonPairNode();
	{
		SP_JsonArrayNode * name = new SP_JsonArrayNode();

		for( int i = 0; i < count; i++ ) {
			const char * value = sqlite3_column_name( stmt, i );

			name->addValue( new SP_JsonStringNode( value ) );
		}

		namePair->setName( "name" );
		namePair->setValue( name );

		rs->addValue( namePair );
	}

	result->addValue( rs );

	sqlite3_finalize( stmt );

	return dbRet;
}

int SP_HiveManager :: doUpdate( sqlite3 * handle, const char * sql, SP_JsonArrayNode * result )
{
	char * errmsg = NULL;

	int dbRet = sqlite3_exec( handle, sql, NULL, NULL, &errmsg );

	if( 0 != dbRet ) {
		SP_NKLog::log( LOG_ERR, "ERROR: sqlite3_exec(%s) = %d, %s",
				sql, dbRet, errmsg ? errmsg : "NULL" );
		if( NULL != errmsg ) sqlite3_free( errmsg );
		return -1;
	}

	int affected = sqlite3_changes( handle );

	SP_JsonObjectNode * rs = new SP_JsonObjectNode();

	SP_JsonPairNode * rowPair = new SP_JsonPairNode();
	{
		SP_JsonArrayNode * rowList = new SP_JsonArrayNode();

		SP_JsonArrayNode * row = new SP_JsonArrayNode();

		SP_JsonIntNode * column = new SP_JsonIntNode( affected );

		row->addValue( column );
		rowList->addValue( row );

		rowPair->setName( "row" );
		rowPair->setValue( rowList );

		rs->addValue( rowPair );
	}

	SP_JsonPairNode * typePair = new SP_JsonPairNode();
	{
		SP_JsonArrayNode * type = new SP_JsonArrayNode();

		type->addValue( new SP_JsonStringNode( "int" ) );

		typePair->setName( "type" );
		typePair->setValue( type );

		rs->addValue( typePair );
	}

	SP_JsonPairNode * namePair = new SP_JsonPairNode();
	{
		SP_JsonArrayNode * name = new SP_JsonArrayNode();

		name->addValue( new SP_JsonStringNode( "affected" ) );

		namePair->setName( "name" );
		namePair->setValue( name );

		rs->addValue( namePair );
	}

	result->addValue( rs );

	return affected;
}

