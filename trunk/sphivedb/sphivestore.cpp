/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <syslog.h>

#include "sphivestore.hpp"
#include "sphivemsg.hpp"

#include "spnetkit/spnklist.hpp"
#include "spnetkit/spnklog.hpp"

SP_HiveStore :: SP_HiveStore()
{
	mArgs = NULL;
	mHandle = NULL;
	mKey = NULL;
}

SP_HiveStore :: ~SP_HiveStore()
{
	if( NULL != mKey ) free( mKey );
	mKey = NULL;
}

void SP_HiveStore :: setArgs( void * args )
{
	mArgs = args;
}

void * SP_HiveStore :: getArgs()
{
	return mArgs;
}

void SP_HiveStore :: setHandle( sqlite3 * handle )
{
	mHandle = handle;
}

sqlite3 * SP_HiveStore :: getHandle()
{
	return mHandle;
}

void SP_HiveStore :: setKey( const char * key )
{
	if( NULL != mKey ) free( mKey );
	mKey = strdup( key );
}

const char * SP_HiveStore :: getKey()
{
	return mKey;
}

//====================================================================

class SP_HiveStoreCache {
public:
	SP_HiveStoreCache( int maxCount, SP_HiveStoreSource * source );
	~SP_HiveStoreCache();

	SP_HiveStore * get( const char * key );

	int save( SP_HiveStore * store );

	static int keycmp( const void * item1, const void * item2 );

private:
	int mMaxCount;
	SP_HiveStoreSource * mSource;

	pthread_mutex_t mMutex;
	SP_NKSortedArray * mItemList;
	SP_NKDoubleLinkList * mTimeList;

	int mTotalGet, mTotalHit;
};

int SP_HiveStoreCache :: keycmp( const void * item1, const void * item2 )
{
	SP_NKDoubleLinkNode_t * n1 = (SP_NKDoubleLinkNode_t*)item1;
	SP_NKDoubleLinkNode_t * n2 = (SP_NKDoubleLinkNode_t*)item2;

	SP_HiveStore * s1 = (SP_HiveStore*)n1->mData;
	SP_HiveStore * s2 = (SP_HiveStore*)n2->mData;

	return strcmp( s1->getKey(), s2->getKey() );
}

SP_HiveStoreCache :: SP_HiveStoreCache( int maxCount, SP_HiveStoreSource * source )
{
	mMaxCount = maxCount;
	mSource = source;

	pthread_mutex_init( &mMutex, NULL );

	mItemList = new SP_NKSortedArray( keycmp );
	mTimeList = new SP_NKDoubleLinkList();

	mTotalHit = mTotalGet = 0;
}

SP_HiveStoreCache :: ~SP_HiveStoreCache()
{
	pthread_mutex_lock( &mMutex );

	for( ; mItemList->getCount() > 0; ) {
		SP_NKDoubleLinkNode_t * node = (SP_NKDoubleLinkNode_t*)mItemList->takeItem( SP_NKVector::LAST_INDEX );
		mTimeList->remove( node );

		delete (SP_HiveStore*)node->mData;
		free( node );
	}

	delete mItemList;
	delete mTimeList;

	pthread_mutex_unlock( &mMutex );

	pthread_mutex_destroy( &mMutex );
}

SP_HiveStore * SP_HiveStoreCache :: get( const char * key )
{
	if( mMaxCount <= 0 ) return NULL;

	SP_HiveStore * ret = NULL;

	pthread_mutex_lock( &mMutex );

	SP_HiveStore keyStore;
	keyStore.setKey( key );

	SP_NKDoubleLinkNode_t keyNode;
	keyNode.mData = &keyStore;

	int index = mItemList->find( &keyNode );

	if( index >= 0 ) {
		SP_NKDoubleLinkNode_t * node = (SP_NKDoubleLinkNode_t*)mItemList->takeItem( index );
		mTimeList->remove( node );

		ret = (SP_HiveStore*)node->mData;

		free( node );

		++mTotalHit;
	}

	++mTotalGet;

	pthread_mutex_unlock( &mMutex );

	if( 0 == ( mTotalGet % 1000 ) ) {
		SP_NKLog::log( LOG_NOTICE, "STAT: HiveStoreCache get %d, hit %d", mTotalGet, mTotalHit );
	}

	return ret;
}

int SP_HiveStoreCache :: save( SP_HiveStore * store )
{
	if( mMaxCount <= 0 ) {
		mSource->close( store );
		delete store;

		return 0;
	}

	pthread_mutex_lock( &mMutex );

	if( mItemList->getCount() > mMaxCount ) {
		SP_NKVector toDeleteList;

		SP_NKDoubleLinkNode_t * node = (SP_NKDoubleLinkNode_t*)mTimeList->getHead();

		for( ; NULL != node; node = node->mNext ) {
			toDeleteList.append( node );

			if( mItemList->getCount() - toDeleteList.getCount() < mMaxCount ) {
				break;
			}
		}

		for( int i = 0; i < toDeleteList.getCount(); i++ ) {
			node = (SP_NKDoubleLinkNode_t*)toDeleteList.getItem( i );

			int index = mItemList->find( node );

			assert( index >= 0 );

			mItemList->takeItem( index );
			mTimeList->remove( node );

			SP_HiveStore * store = (SP_HiveStore*)node->mData;
			mSource->close( store );

			delete store;
			free( node );
		}
	}

	SP_NKDoubleLinkNode_t * newNode = SP_NKDoubleLinkList::newNode();
	newNode->mData = store;

	SP_NKDoubleLinkNode_t * oldNode = NULL;

	mItemList->insert( newNode, (void**)&oldNode );
	assert( NULL == oldNode );

	mTimeList->append( newNode );

	pthread_mutex_unlock( &mMutex );

	return 0;
}

//====================================================================

SP_HiveStoreSource :: ~SP_HiveStoreSource()
{
}

//====================================================================

SP_HiveStoreManager :: SP_HiveStoreManager( SP_HiveStoreSource * source, int maxCacheCount )
{
	mSource = source;
	mCache = new SP_HiveStoreCache( maxCacheCount, source );
}

SP_HiveStoreManager :: ~SP_HiveStoreManager()
{
	delete mCache;
}

SP_HiveStore *  SP_HiveStoreManager :: load( SP_HiveReqObject * req )
{
	SP_HiveStore * store = mCache->get( req->getUniqKey() );

	if( NULL == store ) {
		store = mSource->load( req );
		if( NULL != store ) store->setKey( req->getUniqKey() );
	}

	return store;
}

int SP_HiveStoreManager :: save( SP_HiveReqObject * req, SP_HiveStore * store )
{
	return mSource->save( req, store );
}

int SP_HiveStoreManager :: close( SP_HiveStore * store )
{
	return mCache->save( store );
}

int SP_HiveStoreManager :: remove( SP_HiveReqObject * req )
{
	SP_HiveStore * store = mCache->get( req->getUniqKey() );

	if( NULL != store ) {
		mSource->close( store );
		delete store;
	}

	return mSource->remove( req );
}

