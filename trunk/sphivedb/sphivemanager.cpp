/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>

#include "sphivemanager.hpp"
#include "sphivemsg.hpp"
#include "sphiveconfig.hpp"
#include "sphiveschema.hpp"
#include "sphivestore.hpp"

#include "sqlite3.h"

#include "spnetkit/spnklog.hpp"
#include "spnetkit/spnkporting.hpp"
#include "spnetkit/spnklock.hpp"

#include "spjson/spjsonnode.hpp"
#include "spjson/spjsonrpc.hpp"

SP_HiveManager :: SP_HiveManager()
{
	mLockManager = NULL;
	mStoreManager = NULL;
	mSchemaManager = NULL;

	mConfig = NULL;
}

SP_HiveManager :: ~SP_HiveManager()
{
	if( NULL != mSchemaManager ) delete mSchemaManager, mSchemaManager = NULL;
}

int SP_HiveManager :: init( SP_HiveConfig * config, SP_NKTokenLockManager * lockManager,
		SP_HiveStoreManager * storeManager )
{
	mConfig = config;

	mLockManager = lockManager;
	mStoreManager = storeManager;

	mSchemaManager = new SP_HiveSchemaManager();
	if( 0 != mSchemaManager->init( config ) ) return -1;

	int ret = 0;

	char path[ 256 ] = { 0 };
	for( int i = mConfig->getDBFileBegin(); i <= mConfig->getDBFileEnd(); i += 100 ) {
		int index = i / 100;
		snprintf( path, sizeof( path ), "%s/%d", config->getDataDir(), index );

		if( 0 != access( path, F_OK ) ) {
			if( 0 == mkdir( path, 0700 ) ) {
				SP_NKLog::log( LOG_DEBUG, "Create data dir, %s", path );
			} else {
				SP_NKLog::log( LOG_ERR, "Cannot create data dir, %s", path );
				ret = -1;
			}
		}
	}

	return ret;
}

int SP_HiveManager :: checkReq( SP_HiveReqObject * reqObject,
		SP_JsonObjectNode * errdata )
{
	int dbfile = reqObject->getDBFile();

	if( dbfile < mConfig->getDBFileBegin() || dbfile > mConfig->getDBFileEnd() ) {
		SP_JsonRpcUtils::setError( errdata, -1, "invalid dbfile" );

		SP_NKLog::log( LOG_ERR, "ERROR: invalid dbfile, %d no in [%d, %d]",
				dbfile, mConfig->getDBFileBegin(), mConfig->getDBFileEnd() );
		return -1;
	}

	const char * dbname = reqObject->getDBName();

	const char * ddl = mConfig->getDDL( dbname );

	if( NULL == ddl ) {
		SP_JsonRpcUtils::setError( errdata, -1, "invalid dbname" );

		SP_NKLog::log( LOG_ERR, "ERROR: invalid dbname %s, cannot find ddl", dbname );
		return -1;
	}

	return 0;
}

int SP_HiveManager :: remove( SP_JsonRpcReqObject * rpcReq, SP_JsonObjectNode * errdata )
{
	int ret = 0;

	SP_HiveReqObject reqObject( rpcReq );

	if( 0 != checkReq( &reqObject, errdata ) ) return -1;

	int dbfile = reqObject.getDBFile();
	const char * user = reqObject.getUser();
	const char * dbname = reqObject.getDBName();

	char key4lock[ 128 ] = { 0 };
	snprintf( key4lock, sizeof( key4lock ), "%s/%s", user, dbname );

	SP_NKTokenLockGuard lockGuard( mLockManager );

	if( 0 != lockGuard.lock( key4lock, mConfig->getLockTimeoutSeconds() * 1000 ) ) {
		SP_JsonRpcUtils::setError( errdata, -1, "lock fail" );

		SP_NKLog::log( LOG_ERR, "ERROR: Lock %s fail", user );
		return -1;
	}

	ret = mStoreManager->remove( &reqObject );

	if( ret < 0 ) {
		SP_JsonRpcUtils::setError( errdata, -1, "remove store fail" );
	}

	return ret;
}

int SP_HiveManager :: execute( SP_JsonRpcReqObject * rpcReq,
		SP_JsonArrayNode * result, SP_JsonObjectNode * errdata )
{
	int ret = 0;

	SP_HiveReqObject reqObject( rpcReq );

	if( 0 != checkReq( &reqObject, errdata ) ) return -1;

	int dbfile = reqObject.getDBFile();
	const char * user = reqObject.getUser();
	const char * dbname = reqObject.getDBName();

	char key4lock[ 128 ] = { 0 };
	snprintf( key4lock, sizeof( key4lock ), "%s/%s", user, dbname );

	SP_NKTokenLockGuard lockGuard( mLockManager );

	if( 0 != lockGuard.lock( key4lock, mConfig->getLockTimeoutSeconds() * 1000 ) ) {
		SP_JsonRpcUtils::setError( errdata, -1, "lock fail" );

		SP_NKLog::log( LOG_ERR, "ERROR: Lock %s fail", user );
		return -1;
	}

	SP_HiveStore store;

	ret = mStoreManager->load( &reqObject, &store );

	if( 0 == ret ) {
		ret = mSchemaManager->ensureSchema( store.getHandle(), dbname );
		if( 0 != ret ) SP_JsonRpcUtils::setError( errdata, -1, "ensure schema fail" );
	} else {
		SP_JsonRpcUtils::setError( errdata, -1, "load store fail" );
	}

	int hasUpdate = 0;

	// execute sql script
	if( 0 == ret ) {
		int dbRet = 0;

		dbRet = SP_HiveSchemaManager::execWithLog( store.getHandle(), "BEGIN" );

		for( int i = 0; i < reqObject.getSqlCount(); i++ ) {
			const char * sql = reqObject.getSql( i );
			for( ; isspace( *sql ); ) sql++;

			if( 0 == strncasecmp( "select", sql, 6 ) ) {
				dbRet = doSelect( store.getHandle(), sql, result, errdata );
			} else {
				dbRet = doUpdate( store.getHandle(), sql, result, errdata );
				if( dbRet > 0 ) hasUpdate = 1;
			}

			if( dbRet < 0 ) {
				ret = -1;
				break;
			}
		}

		if( 0 == ret ) {
			dbRet = SP_HiveSchemaManager::execWithLog( store.getHandle(), "COMMIT" );
		} else {
			dbRet = SP_HiveSchemaManager::execWithLog( store.getHandle(), "ROLLBACK" );
		}
	}

	if( 0 == ret && hasUpdate ) {
		ret = mStoreManager->save( &reqObject, &store );
	}

	mStoreManager->close( &store );

	SP_NKLog::log( LOG_DEBUG, "DEBUG: execute( %d, %s, %s, {%d} ) = %d",
			dbfile, user, dbname,  reqObject.getSqlCount(), ret );

	return ret;
}

int SP_HiveManager :: doSelect( sqlite3 * handle, const char * sql,
		SP_JsonArrayNode * result, SP_JsonObjectNode * errdata )
{
	const char * errmsg = NULL;

	sqlite3_stmt * stmt = NULL;

	int dbRet = sqlite3_prepare( handle, sql, strlen( sql ), &stmt, NULL );

	if( SQLITE_OK != dbRet ) {
		errmsg = sqlite3_errmsg( handle );
		SP_JsonRpcUtils::setError( errdata, dbRet, errmsg );

		SP_NKLog::log( LOG_ERR, "ERROR: sqlite3_prepare(%s) = %d, %s", sql, dbRet, errmsg );
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
				errmsg = sqlite3_errmsg( handle );
				SP_JsonRpcUtils::setError( errdata, stepRet, errmsg );

				SP_NKLog::log( LOG_ERR, "ERROR: sqlite3_step(%s) = %d, %s", sql, stepRet, errmsg );

				dbRet = -1;
				break;
			}

			if( SQLITE_DONE == stepRet ) break;

			SP_JsonArrayNode * row = new SP_JsonArrayNode();

			for( int i = 0; i < count; i++ ) {
				const char * value = (char*)sqlite3_column_text( stmt, i );

				if( NULL != value ) {
					SP_JsonStringNode * column = new SP_JsonStringNode( value );
					row->addValue( column );
				} else {
					SP_JsonNullNode * column = new SP_JsonNullNode();
					row->addValue( column );
				}
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

int SP_HiveManager :: doUpdate( sqlite3 * handle, const char * sql,
		SP_JsonArrayNode * result, SP_JsonObjectNode * errdata )
{
	char * errmsg = NULL;

	int dbRet = sqlite3_exec( handle, sql, NULL, NULL, &errmsg );

	if( 0 != dbRet ) {
		SP_JsonRpcUtils::setError( errdata, dbRet, errmsg );

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

