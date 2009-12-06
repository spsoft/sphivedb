<?php
/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

include_once "sphivedbcli.php";

if( $argc != 2 ) {
	printf( "Usage: %s <config file>\n", $argv[0] );
	printf( "\tphp %s ../../sphivedb/sphivedbcli.ini\n", $argv[0] );
	return -1;
}

$configFile = $argv[1];

$client = new SPHiveDBClient();

$ret = $client->init( $configFile );

$resp = $client->remove( 0, "foobar", "addrbook" );

print $resp

?>

