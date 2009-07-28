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
#include "spnetkit/spnkconfig.hpp"

SP_HiveConfig :: SP_HiveConfig()
{
	mListOfDDL = new SP_NKNameValueList();
	mServerConfig = new SP_NKServerConfig();
}

SP_HiveConfig :: ~SP_HiveConfig()
{
	delete mListOfDDL, mListOfDDL = NULL;
	delete mServerConfig, mServerConfig = NULL;
}

int SP_HiveConfig :: init( const char * configFile )
{
	int ret = 0;

	SP_NKIniFile iniFile;

	if( 0 == iniFile.open( configFile ) ) {

		ret = mServerConfig->init( &iniFile, "Server" );

		SP_NKIniItemInfo_t infoArray[] = {
			SP_NK_INI_ITEM_STR( "Database", "DataDir", mDataDir ),
			SP_NK_INI_ITEM_INT( "Database", "DBFileBegin", mDBFileBegin ),
			SP_NK_INI_ITEM_INT( "Database", "DBFileEnd", mDBFileEnd ),
			SP_NK_INI_ITEM_INT( "Database", "MaxOpenFiles", mMaxOpenFiles ),
			SP_NK_INI_ITEM_INT( "Database", "MaxOpenDBs", mMaxOpenDBs ),

			SP_NK_INI_ITEM_INT( "Option", "LockTimeoutSeconds", mLockTimeoutSeconds ),

			SP_NK_INI_ITEM_END
		};

		SP_NKIniFile::BatchLoad( &iniFile, infoArray );

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

			if( key == strstr( key, "ddl." ) && strlen( key ) > 4 ) {
				SP_NKStringList list;
				iniFile.getSection( secName, &list );

				char * text = list.getMerge();

				mListOfDDL->add( key + 4, text );

				SP_NKLog::log( LOG_DEBUG, "INIT: add %s", key );

				free( text );
			}
		}

	} else {
		ret = -1;
	}

	return ret;
}

SP_NKServerConfig * SP_HiveConfig :: getServerConfig()
{
	return mServerConfig;
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

int SP_HiveConfig :: getMaxOpenDBs()
{
	return mMaxOpenDBs;
}

int SP_HiveConfig :: getLockTimeoutSeconds()
{
	return mLockTimeoutSeconds;
}

const char * SP_HiveConfig :: getDDL( const char * dbname )
{
	char key[ 128 ] = { 0 };
	snprintf( key, sizeof( key ), "%s", dbname );

	SP_NKStr::toLower( key );

	return mListOfDDL->getValue( key );
}

SP_NKNameValueList * SP_HiveConfig :: getListOfDDL()
{
	return mListOfDDL;
}

