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
#include "spsqlite.h"

SP_HiveSchemaManager :: SP_HiveSchemaManager()
{
	mConfig = NULL;
	mTableList = NULL;
}

SP_HiveSchemaManager :: ~SP_HiveSchemaManager()
{
	for( int i = 0; i < mTableList->getCount(); i++ ) {
		SP_HiveTableSchema * schema = (SP_HiveTableSchema*)mTableList->getItem( i );
		delete schema;
	}

	if( mTableList ) delete mTableList, mTableList = NULL;
}

int SP_HiveSchemaManager :: createTable( sqlite3 * handle, const char * sql,
			char * table, int size )
{
	char * errmsg = NULL;

	int dbRet = sqlite3_exec( handle, sql, NULL, NULL, &errmsg );

	if( 0 != dbRet ) {
		SP_NKLog::log( LOG_ERR, "ERROR: sqlite3_exec(%s) = %d, %s",
				sql, dbRet, errmsg ? errmsg : "NULL" );
		if( NULL != errmsg ) sqlite3_free( errmsg );
		return -1;
	}

	char tmp[ 512 ] = { 0 };
	snprintf( tmp, sizeof( tmp ),
			"select tbl_name from sqlite_master where type='table'" );

	sqlite3_stmt * stmt = NULL;

	dbRet = sqlite3_prepare( handle, tmp, strlen( tmp ), &stmt, NULL );

	if( SQLITE_OK == dbRet ) {
		dbRet = -1;

		for( ; ; ) {
			int stepRet = sqlite3_step( stmt );

			if( SQLITE_DONE != stepRet && SQLITE_ROW != stepRet ) {
				break;
			}

			if( SQLITE_DONE == stepRet ) break;

			const char * value = (char*)sqlite3_column_text( stmt, 0 );

			if( 0 != strncasecmp( value, "sqlite_", 7 ) ) {
				strncpy( table, value, size );
				table[ size - 1 ] = '\0';
			}

			dbRet = 0;
		}
	}

	sqlite3_finalize( stmt );

	return dbRet;
}

SP_HiveTableSchema * SP_HiveSchemaManager :: findTable( const char * dbname )
{
	SP_HiveTableSchema * ret = NULL;

	for( int i = 0; i < mTableList->getCount(); i++ ) {
		SP_HiveTableSchema * schema = (SP_HiveTableSchema*)mTableList->getItem( i );

		if( 0 == strcasecmp( schema->getDbname(), dbname ) ) {
			ret = schema;
			break;
		}
	}

	return ret;
}

int SP_HiveSchemaManager :: init( SP_HiveConfig * config )
{
	int ret = 0;

	mConfig = config;

	mTableList = new SP_NKVector();

	SP_NKNameValueList * listOfDDL = config->getListOfDDL();

	for( int i = 0; i < listOfDDL->getCount(); i++ ) {
		const char * dbname = listOfDDL->getName( i );
		const char * sql  = listOfDDL->getValue( i );

		sqlite3 * handle = NULL;
		sqlite3_open( ":memory:", &handle );

		char table[ 256 ] = { 0 };
		if( 0 == createTable( handle, sql, table, sizeof( table ) ) ) {
			SP_HiveTableSchema * schema = new SP_HiveTableSchema();
			if( 0 == schema->init( handle, dbname, table ) ) {
				SP_NKLog::log( LOG_DEBUG, "INIT: parse ddl ok, [%s]%s", dbname, sql );
				mTableList->append( schema );
			} else {
				ret = -1;
				SP_NKLog::log( LOG_ERR, "ERROR: invalid ddl, [%s]%s", dbname, sql );
				delete schema;
			}
		} else {
			SP_NKLog::log( LOG_ERR, "ERROR: invalid ddl, [%s]%s", dbname, sql );
			ret = -1;
		}

		sqlite3_close( handle );
	}

	return ret;
}

int SP_HiveSchemaManager :: ensureSchema( sqlite3 * handle, const char * dbname )
{
	const char * newDDL = mConfig->getDDL( dbname );

	SP_HiveTableSchema * newTable = findTable( dbname );

	if( NULL == newDDL || NULL == newTable ) {
		SP_NKLog::log( LOG_ERR, "ERROR: invalid dbname %s, cannot find schema", dbname );
		return -1;
	}

	int ret = 0;

	int oldCount = SP_HiveTableSchema::getTableColumnCount( handle, newTable->getTable() );

	if( oldCount == 0 ) {
		SP_NKLog::log( LOG_DEBUG, "DEBUG: create %s", dbname );
		ret = execWithLog( handle, newDDL );
	} else if( oldCount > 0 ) {
		if( oldCount < newTable->getColumnCount() ) {
			SP_HiveTableSchema oldTable;

			oldTable.init( handle, dbname, newTable->getTable() );

			ret = alterTable( handle, newTable, &oldTable );
		} else {
			// not need action
		}
	} else {
		SP_NKLog::log( LOG_ERR, "ERR: cannot get table column count, [%s]%s",
				dbname, newTable->getTable() );
		ret = -1;
	}

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

int SP_HiveSchemaManager :: alterTable( sqlite3 * handle, SP_HiveTableSchema * newTable,
			SP_HiveTableSchema * oldTable )
{
	SP_NKStringList newColumnList;

	for( int i = 0; i < newTable->getColumnCount(); i++ ) {
		const char * name = newTable->getColumnName( i );

		int index = oldTable->findColumn( name );

		if( index < 0 ) newColumnList.append( newTable->getColumnDefine( i ) );
	}

	int ret = 0;

	if( newColumnList.getCount() > 0 ) {
		for( int i = 0; 0 == ret && i < newColumnList.getCount(); i++ ) {
			SP_NKLog::log( LOG_DEBUG, "DEBUG: %s add column %s",
					oldTable->getTable(), newColumnList.getItem( i ) );

			char sql[ 1024 ] = { 0 };
			snprintf( sql, sizeof( sql ), "alter table %s add column %s;",
					oldTable->getTable(), newColumnList.getItem( i ) );

			ret = execWithLog( handle, sql );
		}
	}

	return ret;
}

//====================================================================

int SP_HiveTableSchema :: getTableColumnCount( sqlite3 * handle, const char * table )
{
	return spsqlite_table_columns_count( handle, table );
}

SP_HiveTableSchema :: SP_HiveTableSchema()
{
	mDbname = NULL;
	mTable = NULL;
	mColumnList = NULL;
}

SP_HiveTableSchema :: ~SP_HiveTableSchema()
{
	if( mDbname ) free( mDbname ), mDbname = NULL;
	if( mTable ) free( mTable ), mTable = NULL;

	if( NULL != mColumnList ) delete mColumnList, mColumnList = NULL;
}

int SP_HiveTableSchema :: init( sqlite3 * handle, const char * dbname, const char * table )
{
	mDbname = strdup( dbname );
	mTable = strdup( table );
	mColumnList = new SP_NKNameValueList();

	spsqlite_column_t * columns = NULL;

	int dbRet = spsqlite_table_columns_get( handle, table, &columns );

	if( 0 != dbRet ) {
		SP_NKLog::log( LOG_ERR, "ERR: spsqlite_table_columns_get %d", dbRet );
		spsqlite_table_columns_free( &columns );
		return -1;
	}

	spsqlite_column_t * iter = columns;
	for( ; NULL != iter; iter = iter->next ) {
		SP_NKStringList columnDefine;

		columnDefine.append( iter->name );
		columnDefine.append( " " );
		columnDefine.append( iter->type );
		columnDefine.append( " " );

		if( iter->notNull ) {
			columnDefine.append( "not null" );
		}
		if( iter->defval ) {
			if( '\0' == *iter->defval ) {
				columnDefine.append( "default \"\"" );
			} else {
				columnDefine.append( "default " );
				columnDefine.append( iter->defval );
			}
		}

		char * value = columnDefine.getMerge();

		mColumnList->add( iter->name, value );

		free( value );
	}

	spsqlite_table_columns_free( &columns );

	return 0;
}

const char * SP_HiveTableSchema :: getDbname() const
{
	return mDbname;
}

const char * SP_HiveTableSchema :: getTable() const
{
	return mTable;
}

int SP_HiveTableSchema :: getColumnCount() const
{
	return mColumnList->getCount();
}

int SP_HiveTableSchema :: findColumn( const char * name )
{
	return mColumnList->seek( name );
}

const char * SP_HiveTableSchema :: getColumnName( int index ) const
{
	return mColumnList->getName( index );
}

const char * SP_HiveTableSchema :: getColumnDefine( int index ) const
{
	return mColumnList->getValue( index );
}

