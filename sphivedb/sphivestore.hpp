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

	void setKey( const char * key );
	const char * getKey();

private:
	void * mArgs;
	sqlite3 * mHandle;
	char * mKey;
};

class SP_HiveStoreSource {
public:
	virtual ~SP_HiveStoreSource();

	virtual SP_HiveStore * load( SP_HiveReqObject * req ) = 0;

	virtual int save( SP_HiveReqObject * req, SP_HiveStore * store ) = 0;

	virtual int close( SP_HiveStore * store ) = 0;

	// @return -1 : Fail, 0 : OK, 1 : No record
	virtual int remove( SP_HiveReqObject * req ) = 0;
};

class SP_HiveStoreCache;

class SP_HiveStoreManager {
public:
	SP_HiveStoreManager( SP_HiveStoreSource * source, int maxCacheCount );
	~SP_HiveStoreManager();

	SP_HiveStore * load( SP_HiveReqObject * req );

	int save( SP_HiveReqObject * req, SP_HiveStore * store );

	int close( SP_HiveStore * store );

	// @return -1 : Fail, 0 : OK, 1 : No record
	int remove( SP_HiveReqObject * req );

private:
	SP_HiveStoreSource * mSource;
	SP_HiveStoreCache * mCache;
};

#endif

