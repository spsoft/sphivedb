/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <assert.h>
#include <fcntl.h>

#include "sphivehandler.hpp"
#include "sphivemanager.hpp"
#include "sphiveconfig.hpp"
#include "spmemvfs.h"

#include "spserver/spserver.hpp"
#include "spserver/splfserver.hpp"
#include "spserver/spporting.hpp"
#include "spserver/spioutils.hpp"

#include "spnetkit/spnklog.hpp"
#include "spnetkit/spnklock.hpp"

void showUsage( const char * program )
{
	printf( "\nUsage: %s [-h <ip>] [-p <port>] [-d] [-f <config>]\n"
			"\t[-s <server mode>] [-x <loglevel>] [-d] [-v]\n", program );
	printf( "\n" );
	printf( "\t-h <ip> listen ip, default is 0.0.0.0\n" );
	printf( "\t-p <port> listen port\n" );
	printf( "\t-d run as daemon\n" );
	printf( "\t-f <config> config file, default is ./spsmtpgate.ini\n" );
	printf( "\t-s <server mode> hahs/lf, half-async/half-sync, leader/follower, default is hahs\n" );
	printf( "\t-x <loglevel> syslog level\n" );
	printf( "\t-v help\n" );
	printf( "\n" );

	exit( 0 );
}

int main( int argc, char * argv[] )
{
	const char * host = "0.0.0.0";
	int port = 0;
	const char * serverMode = "hahs";
	const char * configFile = "spsmtpgate.ini";
	int runAsDaemon = 0;
	int loglevel = LOG_NOTICE;

	extern char *optarg ;
	int c ;

	while( ( c = getopt ( argc, argv, "h:p:s:f:x:dv" )) != EOF ) {
		switch ( c ) {
			case 'h':
				host = optarg;
				break;
			case 'p':
				port = atoi( optarg );
				break;
			case 's':
				serverMode = optarg;
				break;
			case 'f':
				configFile = optarg;
				break;
			case 'x':
				loglevel = atoi( optarg );
				break;
			case 'd':
				runAsDaemon = 1;
				break;
			case '?' :
			case 'v' :
				showUsage( argv[0] );
		}
	}

	if( port <= 0 ) showUsage( argv[0] );

	if( runAsDaemon ) {
		if( 0 != SP_IOUtils::initDaemon() ) {
			printf( "Cannot run as daemon!\n" );
			return -1;
		}
	}

	int logopt = LOG_CONS | LOG_PID;
	if( 0 == runAsDaemon ) logopt |= LOG_PERROR;

	SP_NKLog::setLogLevel( loglevel );
	sp_openlog( "sphivedbsvr", logopt, LOG_USER );

	SP_HiveConfig config;
	config.init( configFile );

	if( 0 != access( config.getDataDir(), F_OK ) ) {
		if( 0 != mkdir( config.getDataDir(), 0700 ) ) {
			SP_NKLog::log( LOG_ERR, "Cannot create data dir, %s", config.getDataDir() );
			return -1;
		}
	}

	if( 0 != access( config.getDataDir(), W_OK | R_OK | X_OK ) ) {
		SP_NKLog::log( LOG_ERR, "Cannot access data dir, %s", config.getDataDir() );
		return -1;
	}

	spmemvfs_env_init();

	SP_NKTokenLockManager lockManager;

	SP_HiveManager manager;
	if( 0 != manager.init( &config, &lockManager ) ) {
		SP_NKLog::log( LOG_ERR, "Cannot init manager" );
		return -1;
	}

	SP_HttpHandlerAdapterFactory * factory =
			new SP_HttpHandlerAdapterFactory( new SP_HiveHandlerFactory( &manager ) );

	if( 0 == strcasecmp( serverMode, "hahs" ) ) {
		SP_Server server( "", port, factory );

		server.setMaxConnections( config.getMaxConnections() );
		server.setTimeout( config.getSocketTimeout() );
		server.setMaxThreads( config.getMaxThreads() );
		server.setReqQueueSize( config.getMaxReqQueueSize(), "HTTP/1.1 500 Sorry, server is busy now!\r\n" );

		server.runForever();
	} else {
		SP_LFServer server( "", port, factory );

		server.setMaxConnections( config.getMaxConnections() );
		server.setTimeout( config.getSocketTimeout() );
		server.setMaxThreads( config.getMaxThreads() );
		server.setReqQueueSize( config.getMaxReqQueueSize(), "HTTP/1.1 500 Sorry, server is busy now!\r\n" );

		server.runForever();
	}

	spmemvfs_env_fini();

	sp_closelog();

	return 0;
}

