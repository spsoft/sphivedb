/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>

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
	mDBList = NULL;
}

SP_HiveSchemaManager :: ~SP_HiveSchemaManager()
{
	if( mDBList ) {
		for( int i = 0; i < mDBList->getCount(); i++ ) {
			SP_HiveDBSchema * schema = (SP_HiveDBSchema*)mDBList->getItem( i );
			delete schema;
		}

		delete mDBList, mDBList = NULL;
	}
}

SP_HiveDBSchema * SP_HiveSchemaManager :: findDB( const char * dbname )
{
	SP_HiveDBSchema * ret = NULL;

	for( int i = 0; i < mDBList->getCount(); i++ ) {
		SP_HiveDBSchema * schema = (SP_HiveDBSchema*)mDBList->getItem( i );

		if( 0 == strcasecmp( schema->getDBName(), dbname ) ) {
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

	mDBList = new SP_NKVector();

	SP_NKNameValueList * listOfDDL = config->getListOfDDL();

	for( int i = 0; i < listOfDDL->getCount(); i++ ) {
		const char * dbname = listOfDDL->getName( i );
		const char * ddl = listOfDDL->getValue( i );

		SP_HiveDBSchema * schema = new SP_HiveDBSchema();
		if( 0 == schema->init( dbname, ddl ) ) {
			mDBList->append( schema );
		} else {
			ret = -1;
			delete schema;
		}
	}

	return ret;
}

int SP_HiveSchemaManager :: ensureSchema( sqlite3 * handle, const char * dbname )
{
	SP_HiveDBSchema * newDB = findDB( dbname );

	if( NULL == newDB ) {
		SP_NKLog::log( LOG_ERR, "ERROR: invalid dbname %s, cannot find schema", dbname );
		return -1;
	}

	int ret = 0;

	for( int i = 0; i < newDB->getTableCount(); i++ ) {
		SP_HiveTableSchema * newTable = newDB->getTable( i );

		int oldCount = SP_HiveTableSchema::getTableColumnCount( handle, newTable->getTableName() );

		if( oldCount == 0 ) {
			SP_NKLog::log( LOG_DEBUG, "DEBUG: create [%s]%s", dbname, newTable->getTableName() );
			ret = execWithLog( handle, newTable->getDDL() );
		} else if( oldCount > 0 ) {
			if( oldCount < newTable->getColumnCount() ) {
				SP_HiveTableSchema oldTable;

				oldTable.init( handle, newTable->getTableName(), "" );

				ret = alterTable( handle, newTable, &oldTable );
			} else {
				// not need action
			}
		} else {
			SP_NKLog::log( LOG_ERR, "ERR: cannot get table column count, [%s]%s",
					dbname, newTable->getTableName() );
			ret = -1;
		}
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
					oldTable->getTableName(), newColumnList.getItem( i ) );

			char sql[ 1024 ] = { 0 };
			snprintf( sql, sizeof( sql ), "alter table %s add column %s;",
					oldTable->getTableName(), newColumnList.getItem( i ) );

			ret = execWithLog( handle, sql );
		}
	}

	return ret;
}

//====================================================================

SP_HiveDBSchema :: SP_HiveDBSchema()
{
	mTableList = NULL;
	mDBName = NULL;
}

SP_HiveDBSchema :: ~SP_HiveDBSchema()
{
	if( NULL != mTableList ) {
		for( int i = 0; i < mTableList->getCount(); i++ ) {
			SP_HiveTableSchema * schema = (SP_HiveTableSchema*)mTableList->getItem( i );
			delete schema;
		}

		delete mTableList, mTableList = NULL;
	}

	if( mDBName ) free( mDBName ), mDBName = NULL;
}

int SP_HiveDBSchema :: init( const char * dbname, const char * ddl )
{
	mTableList = new SP_NKVector();
	mDBName = strdup( dbname );

	int ret = 0;

	sqlite3 * handle = NULL;
	sqlite3_open( ":memory:", &handle );

	for( ; NULL != ddl && 0 == ret; ) {

		sqlite3_stmt * stmt = NULL;
		const char * next = NULL;

		ret = sqlite3_prepare_v2( handle, ddl, strlen( ddl ), &stmt, &next );

		if( SQLITE_OK == ret && NULL != stmt ) {
			const char * sql = sqlite3_sql( stmt );

			char table[ 256 ] = { 0 };
			if( 0 == createTable( handle, sql, table, sizeof( table ) ) ) {
				SP_HiveTableSchema * schema = new SP_HiveTableSchema();
				if( 0 == schema->init( handle, table, sql ) ) {
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
		}

		ddl = stmt ? next : NULL;

		if( NULL != stmt ) sqlite3_finalize( stmt );
	}

	sqlite3_close( handle );

	return ret;
}

int SP_HiveDBSchema :: createTable( sqlite3 * handle, const char * sql,
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
				if( findTable( value ) < 0 ) {
					strncpy( table, value, size );
					table[ size - 1 ] = '\0';
					dbRet = 0;
				}
			}
		}
	}

	sqlite3_finalize( stmt );

	return dbRet;
}

const char * SP_HiveDBSchema :: getDBName() const
{
	return mDBName;
}

int SP_HiveDBSchema :: getTableCount() const
{
	return mTableList->getCount();
}

SP_HiveTableSchema * SP_HiveDBSchema :: getTable( int index ) const
{
	return (SP_HiveTableSchema*)mTableList->getItem( index );
}

int SP_HiveDBSchema :: findTable( const char * tableName )
{
	int ret = -1;

	for( int i = 0; i < mTableList->getCount(); i++ ) {
		SP_HiveTableSchema * schema = (SP_HiveTableSchema*)mTableList->getItem( i );

		if( 0 == strcmp( tableName, schema->getTableName() ) ) {
			ret = i;
			break;
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
	mTableName = NULL;
	mDDL = NULL;
	mColumnList = NULL;
}

SP_HiveTableSchema :: ~SP_HiveTableSchema()
{
	if( mTableName ) free( mTableName ), mTableName = NULL;
	if( mDDL ) free( mDDL ), mDDL = NULL;

	if( NULL != mColumnList ) delete mColumnList, mColumnList = NULL;
}

int SP_HiveTableSchema :: init( sqlite3 * handle, const char * tableName, const char * ddl )
{
	mDDL = strdup( ddl );
	mTableName = strdup( tableName );
	mColumnList = new SP_NKNameValueList();

	spsqlite_column_t * columns = NULL;

	int dbRet = spsqlite_table_columns_get( handle, tableName, &columns );

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

const char * SP_HiveTableSchema :: getTableName() const
{
	return mTableName;
}

const char * SP_HiveTableSchema :: getDDL() const
{
	return mDDL;
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

