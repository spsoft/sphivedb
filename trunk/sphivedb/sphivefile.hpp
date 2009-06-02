/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#ifndef __sphivefile_hpp__
#define __sphivefile_hpp__

#include <pthread.h>

class SP_NKTokenLockManager;
class SP_NKSortedArray;
class SP_NKDoubleLinkList;

class SP_HiveFile {
public:

	const char * getPath();

	void * getDbm();

protected:

	SP_HiveFile( const char * path, void * dbm );
	~SP_HiveFile();

	void addRef();
	void release();
	int getRef();

	friend class SP_HiveFileCache;

private:
	char * mPath;
	void * mDbm;

	int mRefCount;
};

typedef struct tagSP_HiveFileCacheItem SP_HiveFileCacheItem_t;

class SP_HiveFileCache {
public:
	SP_HiveFileCache( int maxCount );
	~SP_HiveFileCache();

	SP_HiveFile * get( const char * path );

	int save( SP_HiveFile * file );

private:

	static int pathcmp( const void * item1, const void * item2 );

private:
	int mMaxCount;

	SP_NKTokenLockManager * mLockManager;

	pthread_mutex_t mMutex;
	SP_NKSortedArray * mItemList;
	SP_NKDoubleLinkList * mTimeList;
};

class SP_HiveFileCacheGuard {
public:
	SP_HiveFileCacheGuard( SP_HiveFileCache * cache );
	~SP_HiveFileCacheGuard();

	SP_HiveFile * get( const char * path );

private:
	SP_HiveFile * mFile;
	SP_HiveFileCache * mCache;
};

#endif

