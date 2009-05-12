/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#ifndef __sphivemanager_hpp__
#define __sphivemanager_hpp__

class SP_JsonRpcReqObject;
class SP_JsonArrayNode;

typedef struct sqlite3 sqlite3;
typedef struct spmembuffer_map_t spmembuffer_map_t;

class SP_HiveManager {
public:
	SP_HiveManager();
	~SP_HiveManager();

	int init( const char * datadir );

	int execute( SP_JsonRpcReqObject * rpcReq, SP_JsonArrayNode * result );

private:

	// for sqlite memvfs
	static void * load( void * arg, const char * path, int * len );
	static int save( void * arg, const char * path, char * buffer, int len );

	static int doSelect( sqlite3 * handle, const char * sql, SP_JsonArrayNode * result );
	static int doUpdate( sqlite3 * handle, const char * sql, SP_JsonArrayNode * result );

private:
	void * mDbm;

	spmembuffer_map_t * mBuffMap;
};

#endif

