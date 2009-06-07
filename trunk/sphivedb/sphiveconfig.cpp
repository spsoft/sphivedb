/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "sphiveconfig.hpp"

#include "spnetkit/spnkini.hpp"
#include "spnetkit/spnklog.hpp"
#include "spnetkit/spnkporting.hpp"
#include "spnetkit/spnklist.hpp"
#include "spnetkit/spnkstr.hpp"

SP_HiveConfig :: SP_HiveConfig()
{
	mListOfDDL = new SP_NKVector();
}

SP_HiveConfig :: ~SP_HiveConfig()
{
	for( int i = 0; i < mListOfDDL->getCount(); i++ ) {
		SP_HiveDDLConfig * iter = (SP_HiveDDLConfig*)mListOfDDL->getItem( i );
		delete iter;
	}

	delete mListOfDDL, mListOfDDL = NULL;
}

int SP_HiveConfig :: init( const char * configFile )
{
	int ret = 0;

	SP_NKIniFile iniFile;

	if( 0 == iniFile.open( configFile ) ) {

		SP_NKIniItemInfo_t infoArray[] = {
			SP_NK_INI_ITEM_INT( "Server", "MaxConnections", mMaxConnections ),
			SP_NK_INI_ITEM_INT( "Server", "MaxThreads", mMaxThreads ),
			SP_NK_INI_ITEM_INT( "Server", "MaxReqQueueSize", mMaxReqQueueSize ),
			SP_NK_INI_ITEM_INT( "Server", "SocketTimeout", mSocketTimeout ),

			SP_NK_INI_ITEM_STR( "Database", "DataDir", mDataDir ),
			SP_NK_INI_ITEM_INT( "Database", "DBFileBegin", mDBFileBegin ),
			SP_NK_INI_ITEM_INT( "Database", "DBFileEnd", mDBFileEnd ),
			SP_NK_INI_ITEM_INT( "Database", "MaxOpenFiles", mMaxOpenFiles ),

			SP_NK_INI_ITEM_INT( "Option", "LockTimeoutSeconds", mLockTimeoutSeconds ),

			SP_NK_INI_ITEM_END
		};

		SP_NKIniFile::BatchLoad( &iniFile, infoArray );

		if( mMaxConnections <= 0 ) mMaxConnections = 128;
		if( mMaxThreads <= 0 ) mMaxThreads = 10;
		if( mMaxReqQueueSize <= 0 ) mMaxReqQueueSize = 100;
		if( mSocketTimeout <= 0 ) mSocketTimeout = 600;

		if( mMaxOpenFiles <= 0 ) mMaxOpenFiles = 16;

		if( mLockTimeoutSeconds <= 0 ) mLockTimeoutSeconds = 20;

		SP_NKIniFile::BatchDump( infoArray );

		SP_NKStringList sectionList;
		iniFile.getSectionNameList( &sectionList );

		for( int i = 0; i < sectionList.getCount(); i++ ) {
			const char * secName = sectionList.getItem( i );

			char key[ 128 ] = { 0 };
			SP_NKStr::strlcpy( key, secName, sizeof( key ) );
			SP_NKStr::toLower( key );

			if( key == strstr( key, "ddl." ) ) {
				SP_NKStringList list;
				iniFile.getSection( secName, &list );

				char * text = list.getMerge();

				SP_HiveDDLConfig * ddl = new SP_HiveDDLConfig();
				if( 0 == ddl->init( key, text ) ) {
					mListOfDDL->append( ddl );
					SP_NKLog::log( LOG_DEBUG, "INIT: add %s", key );
				} else {
					delete ddl;
					SP_NKLog::log( LOG_ERR, "ERROR: invalid sql, [%s]%s", key, text );
				}

				free( text );
			}
		}

	} else {
		ret = -1;
	}

	return ret;
}

int SP_HiveConfig :: getMaxConnections()
{
	return mMaxConnections;
}

int SP_HiveConfig :: getSocketTimeout()
{
	return mSocketTimeout;
}

int SP_HiveConfig :: getMaxThreads()
{
	return mMaxThreads;
}

int SP_HiveConfig :: getMaxReqQueueSize()
{
	return mMaxReqQueueSize;
}

const char * SP_HiveConfig :: getDataDir()
{
	return mDataDir;
}

int SP_HiveConfig :: getDBFileBegin()
{
	return mDBFileBegin;
}

int SP_HiveConfig :: getDBFileEnd()
{
	return mDBFileEnd;
}

int SP_HiveConfig :: getMaxOpenFiles()
{
	return mMaxOpenFiles;
}

int SP_HiveConfig :: getLockTimeoutSeconds()
{
	return mLockTimeoutSeconds;
}

const SP_HiveDDLConfig * SP_HiveConfig :: getDDL( const char * dbname )
{
	char key[ 128 ] = { 0 };
	snprintf( key, sizeof( key ), "ddl.%s", dbname );

	SP_NKStr::toLower( key );

	int index = -1;

	for( int i = 0; i < mListOfDDL->getCount(); i++ ) {
		SP_HiveDDLConfig * iter = (SP_HiveDDLConfig*)mListOfDDL->getItem( i );

		if( 0 == strcmp( key, iter->getName() ) ) {
			index = i;
			break;
		}
	}

	return (SP_HiveDDLConfig*)mListOfDDL->getItem( index );
}

//===========================================================================

int SP_HiveDDLConfig :: computeColumnCount( const char * sql )
{
	int count = 0;

	for( ; '\0' != *sql && '(' != *sql; ) sql++;

	int brackets = 0;

	for( ; '\0' != *sql; sql++ ) {
		if( '(' == *sql ) brackets++;
		if( ')' == *sql ) brackets--;

		if( ')' == *sql && 0 == brackets ) break;

		if( ',' == *sql ) count++;
	}

	return count > 0 ? count + 1 : 0;
}

SP_HiveDDLConfig :: SP_HiveDDLConfig()
{
	mName = NULL;
	mSql = NULL;
	mTable = NULL;

	mColumnList = NULL;
}

SP_HiveDDLConfig :: ~SP_HiveDDLConfig()
{
	if( mName ) free( mName ), mName = NULL;
	if( mSql ) free( mSql ), mSql = NULL;
	if( mTable ) free( mTable ), mTable = NULL;

	if( NULL != mColumnList ) delete mColumnList, mColumnList = NULL;
}

const char * SP_HiveDDLConfig :: getName() const
{
	return mName;
}

int SP_HiveDDLConfig :: init( const char * dbname, const char * sql )
{
	mName = strdup( dbname );
	mSql = strdup( sql );

	mColumnList = new SP_NKNameValueList();

	char buff[ 1024 ] = { 0 };

	SP_NKStr::strlcpy( buff, sql, sizeof( buff ) );

	char * pos = strchr( buff, '(' );

	if( NULL != pos ) {
		for( pos--; pos > buff && isspace( *pos ); pos-- ) *pos = '\0';

		for( ; pos > buff && ( ! isspace( *pos ) ); ) pos--;

		if( '\0' != *pos ) mTable = strdup( isspace( *pos ) ? pos + 1 : pos );
	}

	pos = strchr( sql, '(' );

	for( ; NULL != pos; ) {
		SP_NKStr::strlcpy( buff, ++pos, sizeof( buff ) );

		char * end = strchr( buff, ',' );
		if( NULL == end ) end = strrchr( buff, ')' );
		if( NULL != end ) *end = '\0';

		char tmp[ 128 ] = { 0 };

		SP_NKStr::getToken( buff, 0, tmp, sizeof( tmp ), 0, (const char **)&end );

		if( '\0' != tmp[0] ) mColumnList->add( tmp, end ? end : "" );

		pos = strchr( pos, ',' );
	}

	return NULL != mTable && mColumnList->getCount() > 0 ? 0 : -1;
}

const char * SP_HiveDDLConfig :: getSql() const
{
	return mSql;
}

const char * SP_HiveDDLConfig :: getTable() const
{
	return mTable;
}

int SP_HiveDDLConfig :: getColumnCount() const
{
	return mColumnList->getCount();
}

int SP_HiveDDLConfig :: findColumn( const char * name )
{
	return mColumnList->seek( name );
}

const char * SP_HiveDDLConfig :: getColumnName( int index ) const
{
	return mColumnList->getName( index );
}

const char * SP_HiveDDLConfig :: getColumnType( int index ) const
{
	return mColumnList->getValue( index );
}

