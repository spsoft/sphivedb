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
#include "sphivegather.hpp"

#include "sqlite3.h"

#include "spnetkit/spnklog.hpp"
#include "spnetkit/spnkporting.hpp"
#include "spnetkit/spnklock.hpp"

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

int SP_HiveManager :: checkReq( SP_HiveReqObject * reqObject, SP_HiveRespObjectGather * gather )
{
	int dbfile = reqObject->getDBFile();

	if( dbfile < mConfig->getDBFileBegin() || dbfile > mConfig->getDBFileEnd() ) {
		gather->reportErrdata( -1, "invalid dbfile" );

		SP_NKLog::log( LOG_ERR, "ERROR: invalid dbfile, %d no in [%d, %d]",
				dbfile, mConfig->getDBFileBegin(), mConfig->getDBFileEnd() );
		return -1;
	}

	const char * dbname = reqObject->getDBName();

	const char * ddl = mConfig->getDDL( dbname );

	if( NULL == ddl ) {
		gather->reportErrdata( -1, "invalid dbname" );

		SP_NKLog::log( LOG_ERR, "ERROR: invalid dbname %s, cannot find ddl", dbname );
		return -1;
	}

	return 0;
}

int SP_HiveManager :: remove( SP_HiveReqObject * reqObject, SP_HiveRespObjectGather * gather )
{
	int ret = 0;

	if( 0 != checkReq( reqObject, gather ) ) return -1;

	const char * user = reqObject->getUser();

	SP_NKTokenLockGuard lockGuard( mLockManager );

	if( 0 != lockGuard.lock( reqObject->getUniqKey(), mConfig->getLockTimeoutSeconds() * 1000 ) ) {
		gather->reportErrdata( -1, "lock fail" );

		SP_NKLog::log( LOG_ERR, "ERROR: Lock %s fail", user );
		return -1;
	}

	ret = mStoreManager->remove( reqObject );

	if( ret < 0 ) {
		gather->reportErrdata( -1, "remove store fail" );
	}

	return ret;
}

int SP_HiveManager :: execute( SP_HiveReqObject * reqObject, SP_HiveRespObjectGather * gather )
{
	int ret = 0;

	if( 0 != checkReq( reqObject, gather ) ) return -1;

	int dbfile = reqObject->getDBFile();
	const char * user = reqObject->getUser();
	const char * dbname = reqObject->getDBName();

	SP_NKTokenLockGuard lockGuard( mLockManager );

	if( 0 != lockGuard.lock( reqObject->getUniqKey(), mConfig->getLockTimeoutSeconds() * 1000 ) ) {
		gather->reportErrdata( -1, "lock fail" );

		SP_NKLog::log( LOG_ERR, "ERROR: Lock %s fail", user );
		return -1;
	}

	SP_HiveStore * store = mStoreManager->load( reqObject );

	if( NULL != store ) {
		ret = mSchemaManager->ensureSchema( store->getHandle(), dbname );
		if( 0 != ret ) gather->reportErrdata( -1, "ensure schema fail" );
	} else {
		ret = -1;
		gather->reportErrdata( -1, "load store fail" );
	}

	int hasUpdate = 0;

	// execute sql script
	if( 0 == ret ) {
		int dbRet = 0;

		dbRet = SP_HiveSchemaManager::execWithLog( store->getHandle(), "BEGIN" );

		for( int i = 0; i < reqObject->getSqlCount(); i++ ) {
			const char * sql = reqObject->getSql( i );
			for( ; isspace( *sql ); ) sql++;

			if( 0 == strncasecmp( "select", sql, 6 ) ) {
				dbRet = doSelect( store->getHandle(), sql, gather );
			} else {
				dbRet = doUpdate( store->getHandle(), sql, gather );
				if( dbRet > 0 ) hasUpdate = 1;
			}

			if( dbRet < 0 ) {
				ret = -1;
				break;
			}
		}

		if( 0 == ret ) {
			dbRet = SP_HiveSchemaManager::execWithLog( store->getHandle(), "COMMIT" );
		} else {
			dbRet = SP_HiveSchemaManager::execWithLog( store->getHandle(), "ROLLBACK" );
		}
	}

	if( 0 == ret && hasUpdate ) {
		ret = mStoreManager->save( reqObject, store );
	}

	mStoreManager->close( store );

	SP_NKLog::log( LOG_DEBUG, "DEBUG: execute( %d, %s, %s, {%d} ) = %d",
			dbfile, user, dbname,  reqObject->getSqlCount(), ret );

	return ret;
}

int SP_HiveManager :: doSelect( sqlite3 * handle, const char * sql, SP_HiveRespObjectGather * gather )
{
	const char * errmsg = NULL;

	sqlite3_stmt * stmt = NULL;

	int dbRet = sqlite3_prepare( handle, sql, strlen( sql ), &stmt, NULL );

	if( SQLITE_OK != dbRet ) {
		errmsg = sqlite3_errmsg( handle );
		gather->reportErrdata( dbRet, errmsg );

		SP_NKLog::log( LOG_ERR, "ERROR: sqlite3_prepare(%s) = %d, %s", sql, dbRet, errmsg );
		if( NULL != stmt ) sqlite3_finalize( stmt );
		return -1;
	}

	int count = sqlite3_column_count( stmt );

	{
		for( ; ; ) {
			int stepRet = sqlite3_step( stmt );

			if( SQLITE_DONE != stepRet && SQLITE_ROW != stepRet ) {
				errmsg = sqlite3_errmsg( handle );
				gather->reportErrdata( stepRet, errmsg );

				SP_NKLog::log( LOG_ERR, "ERROR: sqlite3_step(%s) = %d, %s", sql, stepRet, errmsg );

				dbRet = -1;
				break;
			}

			if( SQLITE_DONE == stepRet ) break;

			for( int i = 0; i < count; i++ ) {
				const char * value = (char*)sqlite3_column_text( stmt, i );

				gather->getResultSet()->addColumn( value );
			}

			gather->getResultSet()->submitRow();
		}
	}

	if( 0 != dbRet) return dbRet;

	{
		for( int i = 0; i < count; i++ ) {
			const char * value = sqlite3_column_decltype( stmt, i );
			value = value ? value : "null";

			gather->getResultSet()->addType( value );
		}
	}

	{
		for( int i = 0; i < count; i++ ) {
			const char * value = sqlite3_column_name( stmt, i );

			gather->getResultSet()->addName( value );
		}
	}

	gather->submitResultSet();

	sqlite3_finalize( stmt );

	return dbRet;
}

int SP_HiveManager :: doUpdate( sqlite3 * handle, const char * sql, SP_HiveRespObjectGather * gather )
{
	char * errmsg = NULL;

	int dbRet = sqlite3_exec( handle, sql, NULL, NULL, &errmsg );

	if( 0 != dbRet ) {
		gather->reportErrdata( dbRet, errmsg );

		SP_NKLog::log( LOG_ERR, "ERROR: sqlite3_exec(%s) = %d, %s",
				sql, dbRet, errmsg ? errmsg : "NULL" );
		if( NULL != errmsg ) sqlite3_free( errmsg );
		return -1;
	}

	int affected = sqlite3_changes( handle );
	int last_insert_rowid = sqlite3_last_insert_rowid( handle );

	{
		gather->getResultSet()->addType( "int" );
		gather->getResultSet()->addType( "int" );

		gather->getResultSet()->addName( "affected" );
		gather->getResultSet()->addName( "last_insert_rowid" );

		gather->getResultSet()->addColumn( affected );
		gather->getResultSet()->addColumn( last_insert_rowid );
		gather->getResultSet()->submitRow();
	}

	gather->submitResultSet();

	return affected;
}

