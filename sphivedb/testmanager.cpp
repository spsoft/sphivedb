/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <string.h>

#include "sphivemanager.hpp"
#include "sphivemsg.hpp"
#include "sphiveconfig.hpp"
#include "spdbmstore.hpp"
#include "sphivegather.hpp"

#include "spmemvfs.h"

#include "spjson/spjsonnode.hpp"
#include "spjson/spjsonrpc.hpp"
#include "spjson/spjsonutils.hpp"

#include "spnetkit/spnklock.hpp"
#include "spnetkit/spnklog.hpp"
#include "spnetkit/spnkporting.hpp"

void testExecute( SP_HiveManager * manager, const char * buffer, int len )
{
	SP_JsonRpcReqObject rpcReq( buffer, len );

	if( NULL != rpcReq.getPacketError() ) {
		printf( "packet.error %s\n", rpcReq.getPacketError() );
		return;
	}

	SP_HiveReqObjectJson reqObject( &rpcReq );

	SP_HiveRespObjectGatherJson respObject;

	manager->execute( &reqObject, &respObject );

	SP_JsonStringBuffer respBuffer;
	SP_JsonRpcUtils::toRespBuffer( rpcReq.getID(), respObject.getResult(),
			NULL, &respBuffer );

	printf( "%s\n", respBuffer.getBuffer() );
}

int main( int argc, char * argv[] )
{
	if( argc < 2 ) {
		printf( "Usage: %s <req file>\n", argv[0] );
		exit( -1 );
	}

	const char * filename = argv[1];

	FILE * fp = fopen ( filename, "r" );
	if( NULL == fp ) {
		printf( "cannot not open %s\n", filename );
		exit( -1 );
	}

	struct stat aStat;
	char * source = NULL;
	stat( filename, &aStat );
	source = ( char * ) malloc ( aStat.st_size + 1 );
	fread ( source, aStat.st_size, sizeof ( char ), fp );
	fclose ( fp );
	source[ aStat.st_size ] = '\0';

	int logopt = LOG_CONS | LOG_PID | LOG_PERROR;

	SP_NKLog::setLogLevel( 7 );
	openlog( "testmanager", logopt, LOG_USER );

	spmemvfs_env_init();

	SP_HiveConfig config;
	config.init( "./sphivedbsvr.ini" );

	SP_NKTokenLockManager lockManager;

	SP_DbmStoreSource storeSource;
	storeSource.init( &config );

	SP_HiveStoreManager storeManager( &storeSource, config.getMaxOpenDBs() );

	SP_HiveManager manager;
	manager.init( &config, &lockManager, &storeManager );

	testExecute( &manager, source, aStat.st_size );

	free( source );

	spmemvfs_env_fini();

	return 0;
}

