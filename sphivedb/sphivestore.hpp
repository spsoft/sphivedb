/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#ifndef __sphivestore_hpp__
#define __sphivestore_hpp__

typedef struct sqlite3 sqlite3;

class SP_HiveReqObject;

class SP_HiveStore {
public:
	SP_HiveStore();
	~SP_HiveStore();

	void setArgs( void * args );
	void * getArgs();

	void setHandle( sqlite3 * handle );
	sqlite3 * getHandle();

private:
	void * mArgs;
	sqlite3 * mHandle;
};

class SP_HiveStoreManager {
public:
	virtual ~SP_HiveStoreManager();

	virtual int load( SP_HiveReqObject * req, SP_HiveStore * store ) = 0;

	virtual int save( SP_HiveReqObject * req, SP_HiveStore * store ) = 0;

	virtual int close( SP_HiveStore * store ) = 0;

	// @return -1 : Fail, 0 : OK, 1 : No record
	virtual int remove( SP_HiveReqObject * req ) = 0;
};

#endif

