/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#ifndef __sphivedbcli_hpp__
#define __sphivedbcli_hpp__

#include <sys/types.h>

class SP_NKSocket;
class SP_NKStringList;

class SP_HiveRespObject;

class SP_HiveDBProtocol {
public:

	SP_HiveDBProtocol( SP_NKSocket * socket, int isKeepAlive );

	SP_HiveDBProtocol();

	SP_HiveRespObject * execute( const char * path, SP_NKStringList * sql );

private:
	SP_NKSocket * mSocket;
	int mIsKeepAlive;
};

#endif

