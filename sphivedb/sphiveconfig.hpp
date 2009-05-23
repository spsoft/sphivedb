/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#ifndef __sphiveconfig_hpp__
#define __sphiveconfig_hpp__

class SP_HiveConfig {
public:
	SP_HiveConfig();
	~SP_HiveConfig();

	int init( const char * configFile );

	int getMaxConnections();
	int getSocketTimeout();
	int getMaxThreads();
	int getMaxReqQueueSize();

	const char * getDataDir();

	int getLockTimeoutSeconds();

private:
	int mMaxConnections;
	int mSocketTimeout;
	int mMaxThreads;
	int mMaxReqQueueSize;

	char mDataDir[ 256 ];

	int mLockTimeoutSeconds;
};

#endif

