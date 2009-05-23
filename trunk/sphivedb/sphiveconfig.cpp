/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#include <stdlib.h>
#include <stdio.h>

#include "sphiveconfig.hpp"

#include "spnetkit/spnkini.hpp"
#include "spnetkit/spnklog.hpp"

SP_HiveConfig :: SP_HiveConfig()
{
}

SP_HiveConfig :: ~SP_HiveConfig()
{
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

			SP_NK_INI_ITEM_INT( "Option", "LockTimeoutSeconds", mLockTimeoutSeconds ),

			SP_NK_INI_ITEM_END
		};

		SP_NKIniFile::BatchLoad( &iniFile, infoArray );

		if( mMaxConnections <= 0 ) mMaxConnections = 128;
		if( mMaxThreads <= 0 ) mMaxThreads = 10;
		if( mMaxReqQueueSize <= 0 ) mMaxReqQueueSize = 100;
		if( mSocketTimeout <= 0 ) mSocketTimeout = 600;

		if( mLockTimeoutSeconds <= 0 ) mLockTimeoutSeconds = 20;

		SP_NKIniFile::BatchDump( infoArray );
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

int SP_HiveConfig :: getLockTimeoutSeconds()
{
	return mLockTimeoutSeconds;
}

