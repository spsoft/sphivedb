/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "sphivemanager.hpp"
#include "sphivemsg.hpp"
#include "sphiveconfig.hpp"

#include "spmemvfs.h"

#include "spcabinet.h"

#include "sqlite3.h"

#include "spnetkit/spnklog.hpp"
#include "spnetkit/spnkporting.hpp"
#include "spnetkit/spnklock.hpp"

#include "spjson/spjsonnode.hpp"

SP_HiveManager :: SP_HiveManager()
{
	mLockManager = NULL;
	mConfig = NULL;
	mDbm = NULL;
}

SP_HiveManager :: ~SP_HiveManager()
{
	if( NULL != mDbm ) {
		sp_tcadbclose( mDbm );
		sp_tcadbdel( mDbm );
	}

	mDbm = NULL;
}

int SP_HiveManager :: init( SP_HiveConfig * config, SP_NKTokenLockManager * lockManager )
{
	mConfig = config;
	mLockManager = lockManager;

	char name[ 256 ] = { 0 };

	snprintf( name, sizeof( name ), "%s/sphive0.tch", config->getDataDir() );

	mDbm = sp_tcadbnew();

	if( ! sp_tcadbopen( mDbm, name ) ) {
		sp_tcadbdel( mDbm );
		mDbm = NULL;
		SP_NKLog::log( LOG_ERR, "ERROR: sp_tcadbopen fail" );
	}

	return NULL != mDbm ? 0 : -1;
}

int SP_HiveManager :: execute( SP_JsonRpcReqObject * rpcReq, SP_JsonArrayNode * result )
{
	int ret = 0;

	SP_HiveReqObject reqObject( rpcReq );
	const char * path = reqObject.getPath();

	SP_NKTokenLockGuard lockGuard( mLockManager );

	if( 0 != lockGuard.lock( path, mConfig->getLockTimeoutSeconds() * 1000 ) ) {
		SP_NKLog::log( LOG_ERR, "ERROR: Lock %s fail", path );
		return -1;
	}

	spmemvfs_db_t * db = (spmemvfs_db_t*)calloc( sizeof( spmemvfs_db_t ), 1 );

	// load buffer from cabinet, and open db
	{
		spmembuffer_t * mem = (spmembuffer_t*)calloc( sizeof( spmembuffer_t ), 1 );

		int vlen = 0;
		void * vbuf = sp_tcadbget( mDbm, path, strlen( path ), &vlen );

		if( NULL != vbuf ) {
			mem->data = (char*)vbuf;
			mem->used = mem->total = vlen;
		}

		spmemvfs_open_db( db, path, mem );
	}

	int hasUpdate = 0;

	// execute sql script
	{
		int dbRet = 0;

		for( int i = 0; i < reqObject.getSqlCount(); i++ ) {
			const char * sql = reqObject.getSql( i );

			if( 0 == strncasecmp( "select", sql, 6 ) ) {
				dbRet = doSelect( db->handle, sql, result );
			} else {
				dbRet = doUpdate( db->handle, sql, result );
				if( dbRet > 0 ) hasUpdate = 1;
			}

			if( dbRet < 0 ) {
				ret = -1;
				break;
			}
		}
	}

	// write buffer to cabinet
	{
		if( NULL != db->mem->data ) {
			if( 0 == ret && hasUpdate ) {
				if( ! sp_tcadbput( mDbm, path, strlen( path ), db->mem->data, db->mem->used ) ) {
					SP_NKLog::log( LOG_ERR, "ERROR: tcadbput fail" );
					ret = -1;
				}
			}
		}

		spmemvfs_close_db( db );
		free( db );
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
	int last_insert_rowid = sqlite3_last_insert_rowid( handle );

	SP_JsonPairNode * rowPair = new SP_JsonPairNode();
	{
		SP_JsonArrayNode * rowList = new SP_JsonArrayNode();

		SP_JsonArrayNode * row = new SP_JsonArrayNode();

		SP_JsonIntNode * column = new SP_JsonIntNode( affected );
		row->addValue( column );

		column = new SP_JsonIntNode( last_insert_rowid );
		row->addValue( column );

		rowList->addValue( row );

		rowPair->setName( "row" );
		rowPair->setValue( rowList );
	}

	SP_JsonPairNode * typePair = new SP_JsonPairNode();
	{
		SP_JsonArrayNode * type = new SP_JsonArrayNode();

		type->addValue( new SP_JsonStringNode( "int" ) );
		type->addValue( new SP_JsonStringNode( "int" ) );

		typePair->setName( "type" );
		typePair->setValue( type );
	}

	SP_JsonPairNode * namePair = new SP_JsonPairNode();
	{
		SP_JsonArrayNode * name = new SP_JsonArrayNode();

		name->addValue( new SP_JsonStringNode( "affected" ) );
		name->addValue( new SP_JsonStringNode( "last_insert_rowid" ) );

		namePair->setName( "name" );
		namePair->setValue( name );
	}

	SP_JsonObjectNode * rs = new SP_JsonObjectNode();
	rs->addValue( namePair );
	rs->addValue( typePair );
	rs->addValue( rowPair );

	result->addValue( rs );

	return affected;
}

