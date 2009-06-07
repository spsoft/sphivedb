/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#include <stdio.h>
#include <assert.h>
#include <stdlib.h>

#include "spnetkit/spnklog.hpp"
#include "spnetkit/spnkporting.hpp"
#include "spnetkit/spnklist.hpp"

#include "sphiveschema.hpp"
#include "sphiveconfig.hpp"

#include "sqlite3.h"

SP_HiveSchemaManager :: SP_HiveSchemaManager( SP_HiveConfig * config )
{
	mConfig = config;
}

SP_HiveSchemaManager :: ~SP_HiveSchemaManager()
{
}

int SP_HiveSchemaManager :: getTableSql( sqlite3 * handle,
		const char * table, char ** sql )
{
	char tmp[ 512 ] = { 0 };
	snprintf( tmp, sizeof( tmp ),
			"select sql from sqlite_master where type='table' and tbl_name='%s'", table );

	sqlite3_stmt * stmt = NULL;

	int dbRet = sqlite3_prepare( handle, tmp, strlen( tmp ), &stmt, NULL );

	if( SQLITE_OK != dbRet ) {
		SP_NKLog::log( LOG_ERR, "ERROR: sqlite3_prepare(%s) = %d", tmp, dbRet );

		if( NULL != stmt ) sqlite3_finalize( stmt );
		return -1;
	}

	dbRet = 1; // no record

	for( ; ; ) {
		int stepRet = sqlite3_step( stmt );

		if( SQLITE_DONE != stepRet && SQLITE_ROW != stepRet ) {
			dbRet = -1;
			SP_NKLog::log( LOG_ERR, "ERROR: sqlite3_step(%s) = %d", sql, stepRet );
			break;
		}

		if( SQLITE_DONE == stepRet ) break;

		const char * value = (char*)sqlite3_column_text( stmt, 0 );

		*sql = strdup( value );

		dbRet = 0;
	}

	sqlite3_finalize( stmt );

	return dbRet;
}

int SP_HiveSchemaManager :: ensureSchema( sqlite3 * handle, const char * dbname )
{
	const SP_HiveDDLConfig * ddl = mConfig->getDDL( dbname );

	if( NULL == ddl ) {
		SP_NKLog::log( LOG_ERR, "ERROR: invalid dbname %s, cannot find ddl", dbname );
		return -1;
	}

	int ret = 0;

	char * sql = NULL;

	ret = getTableSql( handle, ddl->getTable(), &sql );

	if( ret > 0 ) {
		SP_NKLog::log( LOG_DEBUG, "DEBUG: create %s", dbname );
		ret = execWithLog( handle, ddl->getSql() );
	} else if( 0 == ret ) {
		if( NULL != sql ) {
			ret = updateIfNeed( handle, ddl, sql );
		} else {
			ret = -1;
		}
	}

	if( NULL != sql ) free( sql );

	return ret;
}

int SP_HiveSchemaManager :: execWithLog( sqlite3 * handle, const char * sql )
{
	char * errmsg = NULL;

	int ret = sqlite3_exec( handle, sql, NULL, NULL, &errmsg );

	if( 0 != ret ) {
		SP_NKLog::log( LOG_ERR, "ERROR: sqlite3_exec(%s) = %d, %s",
				sql, ret, errmsg ? errmsg : "NULL" );
		if( NULL != errmsg ) sqlite3_free( errmsg );
	}

	return ret;
}

int SP_HiveSchemaManager :: updateIfNeed( sqlite3 * handle, const SP_HiveDDLConfig * ddl,
			const char * oldSql )
{
	int ret = 0;

	int count = SP_HiveDDLConfig::computeColumnCount( oldSql );

	if( ddl->getColumnCount() > count ) {
		SP_HiveDDLConfig old;
		assert( 0 == old.init( "old", oldSql ) );

		SP_NKNameValueList newColumnList;

		for( int i = 0; i < ddl->getColumnCount(); i++ ) {
			const char * name = ddl->getColumnName( i );

			int index = old.findColumn( name );

			if( index < 0 ) newColumnList.add( name, ddl->getColumnType( i ) );
		}

		if( newColumnList.getCount() > 0 ) {
			for( int i = 0; 0 == ret && i < newColumnList.getCount(); i++ ) {
				SP_NKLog::log( LOG_DEBUG, "DEBUG: add column %s.%s",
						ddl->getTable(), newColumnList.getName(i) );

				char sql[ 1024 ] = { 0 };
				snprintf( sql, sizeof( sql ), "alter table %s add column %s %s",
						ddl->getTable(), newColumnList.getName( i ), newColumnList.getValue( i ) );

				ret = execWithLog( handle, sql );
			}
		}
	}

	return ret;
}

