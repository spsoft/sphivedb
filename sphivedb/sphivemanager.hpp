/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#ifndef __sphivemanager_hpp__
#define __sphivemanager_hpp__

class SP_JsonRpcReqObject;
class SP_JsonArrayNode;
class SP_NKTokenLockManager;
class SP_HiveConfig;

typedef struct sqlite3 sqlite3;

class SP_HiveManager {
public:
	SP_HiveManager();
	~SP_HiveManager();

	int init( SP_HiveConfig * config, SP_NKTokenLockManager * lockManager );

	int execute( SP_JsonRpcReqObject * rpcReq, SP_JsonArrayNode * result );

private:

	static int doSelect( sqlite3 * handle, const char * sql, SP_JsonArrayNode * result );
	static int doUpdate( sqlite3 * handle, const char * sql, SP_JsonArrayNode * result );

private:
	SP_NKTokenLockManager * mLockManager;
	SP_HiveConfig * mConfig;
	void * mDbm;
};

#endif

