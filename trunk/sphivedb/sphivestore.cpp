/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#include <stdio.h>

#include "sphivestore.hpp"
#include "sphivemsg.hpp"

SP_HiveStore :: SP_HiveStore()
{
	mArgs = NULL;
	mHandle = NULL;
}

SP_HiveStore :: ~SP_HiveStore()
{
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

//====================================================================

SP_HiveStoreManager :: ~SP_HiveStoreManager()
{
}

