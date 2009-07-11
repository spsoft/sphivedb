/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>

#include "spfilestore.hpp"

#include "sphiveconfig.hpp"
#include "sphivemsg.hpp"

#include "sqlite3.h"

#include "spnetkit/spnklog.hpp"
#include "spserver/spporting.hpp"

SP_FileStoreManager :: SP_FileStoreManager()
{
}

SP_FileStoreManager :: ~SP_FileStoreManager()
{
}

int SP_FileStoreManager :: init( SP_HiveConfig * config )
{
	mConfig = config;

	return 0;
}

int SP_FileStoreManager :: load( SP_HiveReqObject * req, SP_HiveStore * store )
{
	char dpath[ 256 ] = { 0 }, fpath[ 256] = { 0 };
	getPath( req, dpath, sizeof( dpath ), fpath, sizeof( fpath ) );

	if( 0 != access( dpath, F_OK ) ) {
		if( 0 != mkdir( dpath, 0700 ) ) {
			SP_NKLog::log( LOG_ERR, "Cannot create data dir, %s", dpath );
			return -1;
		}
	}

	int ret = 0;

	sqlite3 * handle = NULL;

	if( SQLITE_OK != sqlite3_open( fpath, &handle ) ) {
		SP_NKLog::log( LOG_ERR, "Cannot open db %s, %s", fpath,
				handle ? sqlite3_errmsg( handle ) : "OutOfMemory" );
		ret = -1;
	}

	if( 0 == ret && NULL != handle ) {
		store->setHandle( handle );
	} else {
		if( NULL != handle ) sqlite3_close( handle );
	}

	return ret;
}

int SP_FileStoreManager :: save( SP_HiveReqObject * req, SP_HiveStore * store )
{
	int ret = 0;

	return ret;
}

int SP_FileStoreManager :: close( SP_HiveStore * store )
{
	int ret = 0;

	sqlite3_close( store->getHandle() );

	store->setHandle( NULL );

	return ret;
}

int SP_FileStoreManager :: remove( SP_HiveReqObject * req )
{
	char dpath[ 256 ] = { 0 }, fpath[ 256] = { 0 };
	getPath( req, dpath, sizeof( dpath ), fpath, sizeof( fpath ) );

	if( 0 != access( fpath, F_OK ) ) {
		return 1;
	}

	if( 0 != unlink( fpath ) ) {
		SP_NKLog::log( LOG_ERR, "unlink %s fail, errno %d, %s",
				fpath, errno, strerror( errno ) );
		return -1;
	}

	return 0;
}

int SP_FileStoreManager :: getPath( SP_HiveReqObject * req,
		char * dpath, int dsize, char  * fpath, int fsize )
{
	snprintf( dpath, dsize, "%s/%d/%d", mConfig->getDataDir(),
			req->getDBFile() / 100, req->getDBFile() );
	
	snprintf( fpath, fsize, "%s/%d/%d/%s.%s.db", mConfig->getDataDir(),
			req->getDBFile() / 100, req->getDBFile(), req->getDBName(), req->getUser() );

	return 0;
}

