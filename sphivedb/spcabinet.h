/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#ifndef __spcabinet_h__
#define __spcabinet_h__

#ifdef __cplusplus
extern "C" {
#endif

void * sp_tcadbnew();

void sp_tcadbdel( void * adb );

int sp_tcadbopen( void * adb, const char *name );

int sp_tcadbclose( void * adb );

int sp_tcadbput( void * adb, const void *kbuf, int ksiz, const void *vbuf, int vsiz );

void * sp_tcadbget( void * adb, const void *kbuf, int ksiz, int *sp );

#ifdef __cplusplus
}
#endif

#endif

