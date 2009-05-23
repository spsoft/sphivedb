/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#include "spcabinet.h"

#include "tcadb.h" // tokyocabinet

void * sp_tcadbnew()
{
	return tcadbnew();
}

void sp_tcadbdel( void * adb )
{
	tcadbdel( adb );
}

int sp_tcadbopen( void * adb, const char *name )
{
	return tcadbopen( adb, name );
}

int sp_tcadbclose( void * adb )
{
	return tcadbclose( adb );
}

int sp_tcadbput( void * adb, const void *kbuf, int ksiz, const void *vbuf, int vsiz )
{
	return tcadbput( adb, kbuf, ksiz, vbuf, vsiz );
}

void * sp_tcadbget( void * adb, const void *kbuf, int ksiz, int *sp )
{
	return tcadbget( adb, kbuf, ksiz, sp );
}

