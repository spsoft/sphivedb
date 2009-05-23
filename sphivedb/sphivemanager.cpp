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
	mDbmList = NULL;
}

SP_HiveManager :: ~SP_HiveManager()
{
	if( NULL != mDbmList ) {
		int count = mConfig->getDBFileEnd() - mConfig->getDBFileBegin() + 1;

		for( int i = 0; i < count; i++ ) {
			if( NULL != mDbmList[i] ) {
				sp_tcadbclose( mDbmList[i] );
				sp_tcadbdel( mDbmList[i] );
			}
		}

		free( mDbmList );
	}

	mDbmList = NULL;
}

int SP_HiveManager :: init( SP_HiveConfig * config, SP_NKTokenLockManager * lockManager )
{
	mConfig = config;
	mLockManager = lockManager;

	int ret = 0;

	int count = mConfig->getDBFileEnd() - mConfig->getDBFileBegin() + 1;
	mDbmList = (void**)calloc( sizeof( void * ), count );

	for( int i = 0; i < count; i++ ) {
		char name[ 256 ] = { 0 };

		snprintf( name, sizeof( name ), "%s/sphive%d.tch",
				config->getDataDir(), i + mConfig->getDBFileBegin() );

		void * adb = sp_tcadbnew();
		if( ! sp_tcadbopen( adb, name ) ) {
			sp_tcadbdel( adb );
			SP_NKLog::log( LOG_ERR, "ERROR: sp_tcadbopen fail" );

			ret = -1;
			break;
		}

		mDbmList[i] = adb;
	}

	return ret;
}

int SP_HiveManager :: checkReq( SP_HiveReqObject * reqObject )
{
	int dbfile = reqObject->getDBFile();

	if( dbfile < mConfig->getDBFileBegin() || dbfile > mConfig->getDBFileEnd() ) {
		SP_NKLog::log( LOG_ERR, "ERROR: invalid dbfile, %d no in [%d, %d]",
				dbfile, mConfig->getDBFileBegin(), mConfig->getDBFileEnd() );
		return -1;
	}

	if( NULL == mDbmList || NULL == mDbmList[ dbfile - mConfig->getDBFileBegin() ] ) {
		SP_NKLog::log( LOG_ERR, "ERROR: server internal error, invalid status" );
		return -1;
	}

	const char * dbname = reqObject->getDBName();

	const char * ddl = mConfig->getDDL( dbname );

	if( NULL == ddl ) {
		SP_NKLog::log( LOG_ERR, "ERROR: invalid dbname %s, cannot find ddl", dbname );
		return -1;
	}

	return 0;
}

int SP_HiveManager :: load( void * dbm, const char * path, spmemvfs_db_t * db, const char * ddl )
{
	spmembuffer_t * mem = (spmembuffer_t*)calloc( sizeof( spmembuffer_t ), 1 );

	int vlen = 0;
	void * vbuf = sp_tcadbget( dbm, path, strlen( path ), &vlen );

	if( NULL != vbuf ) {
		mem->data = (char*)vbuf;
		mem->used = mem->total = vlen;
	}

	if( 0 != spmemvfs_open_db( db, path, mem ) ) {
		SP_NKLog::log( LOG_ERR, "ERROR: cannot open db, %s", path );
		return -1;
	}

	if( 0 == vlen ) {
		char * errmsg = NULL;

		int dbRet = sqlite3_exec( db->handle, ddl, NULL, NULL, &errmsg );

		if( 0 != dbRet ) {
			SP_NKLog::log( LOG_ERR, "ERROR: sqlite3_exec(%s) = %d, %s",
					ddl, dbRet, errmsg ? errmsg : "NULL" );
			if( NULL != errmsg ) sqlite3_free( errmsg );

			spmemvfs_close_db( db );

			return -1;
		}
	}

	return 0;
}

int SP_HiveManager :: execute( SP_JsonRpcReqObject * rpcReq, SP_JsonArrayNode * result )
{
	int ret = 0;

	SP_HiveReqObject reqObject( rpcReq );

	if( 0 != checkReq( &reqObject ) ) return -1;

	int dbfile = reqObject.getDBFile();
	const char * user = reqObject.getUser();
	const char * dbname = reqObject.getDBName();

	void * dbm = mDbmList[ dbfile - mConfig->getDBFileBegin() ];
	const char * ddl = mConfig->getDDL( dbname );

	char path[ 256 ] = { 0 };
	snprintf( path, sizeof( path ), "%s/%s", user, dbname );

	SP_NKTokenLockGuard lockGuard( mLockManager );

	if( 0 != lockGuard.lock( path, mConfig->getLockTimeoutSeconds() * 1000 ) ) {
		SP_NKLog::log( LOG_ERR, "ERROR: Lock %s fail", path );
		return -1;
	}

	spmemvfs_db_t * db = (spmemvfs_db_t*)calloc( sizeof( spmemvfs_db_t ), 1 );

	ret = load( dbm, path, db, ddl );

	int hasUpdate = 0;

	// execute sql script
	if( 0 == ret ) {
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
	if( 0 == ret ) {
		if( NULL != db->mem->data ) {
			if( 0 == ret && hasUpdate ) {
				if( ! sp_tcadbput( dbm, path, strlen( path ), db->mem->data, db->mem->used ) ) {
					SP_NKLog::log( LOG_ERR, "ERROR: tcadbput fail" );
					ret = -1;
				}
			}
		}
	}

	spmemvfs_close_db( db );
	free( db );

	SP_NKLog::log( LOG_DEBUG, "DEBUG: execute( %d, %s, %s, {%d} ) = %d",
			dbfile, user, dbname,  reqObject.getSqlCount(), ret );

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

	if( 0 != dbRet) return dbRet;

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

