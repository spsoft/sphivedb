/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#ifndef __spfilestore_hpp__
#define __spfilestore_hpp__

#include "sphivestore.hpp"

class SP_HiveConfig;
class SP_HiveReqObject;

class SP_FileStoreManager : public SP_HiveStoreManager {
public:
	SP_FileStoreManager();
	virtual ~SP_FileStoreManager();

	int init( SP_HiveConfig * config );

	virtual int load( SP_HiveReqObject * req, SP_HiveStore * store );

	virtual int save( SP_HiveReqObject * req, SP_HiveStore * store );

	virtual int close( SP_HiveStore * store );

private:
	int getPath( SP_HiveReqObject * req, char * dpath, int dsize,
			char  * fpath, int fsize );

private:
	SP_HiveConfig * mConfig;
};

#endif

