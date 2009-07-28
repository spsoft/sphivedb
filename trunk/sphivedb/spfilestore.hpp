/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#ifndef __spfilestore_hpp__
#define __spfilestore_hpp__

#include "sphivestore.hpp"

class SP_HiveConfig;
class SP_HiveReqObject;

class SP_FileStoreSource : public SP_HiveStoreSource {
public:
	SP_FileStoreSource();
	virtual ~SP_FileStoreSource();

	int init( SP_HiveConfig * config );

	virtual SP_HiveStore * load( SP_HiveReqObject * req );

	virtual int save( SP_HiveReqObject * req, SP_HiveStore * store );

	virtual int close( SP_HiveStore * store );

	virtual int remove( SP_HiveReqObject * req );

private:
	int getPath( SP_HiveReqObject * req, char * dpath, int dsize,
			char  * fpath, int fsize );

private:
	SP_HiveConfig * mConfig;
};

#endif

