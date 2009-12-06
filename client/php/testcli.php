<?php
/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

include_once "sphivedbcli.php";

function printResultSet( $rs ) {
	printf( "row.count %d\n", $rs->getRowCount() );

	$columnCount = $rs->getColumnCount();

	$hdrs = "";
	for( $i = 0; $i < $columnCount; $i++ ) {
		$hdrs = $hdrs . "\t" . $rs->getName( $i ) . "(" . $rs->getType( $i ) . ")";
	}

	printf( "%s\n", $hdrs );

	$rowCount = $rs->getRowCount();

	for( $i = 0; $i < $rowCount; $i++ ) {
		$rs->moveTo( $i );

		$row = "";

		for( $j = 0; $j < $columnCount; $j++ ) {
			$row = $row . "\t[" . $rs->getField( $j ) . "]";
		}

		printf( "%s\n", $row );
	}
}

if( $argc != 2 ) {
	printf( "Usage: %s <config file>\n", $argv[0] );
	printf( "\tphp %s ../../sphivedb/sphivedbcli.ini\n", $argv[0] );
	return -1;
}

$configFile = $argv[1];

$client = new SPHiveDBClient();

$ret = $client->init( $configFile );

$respObj = $client->execute( 0, "foobar", "addrbook",
		array( "insert into addrbook ( addr ) values ( \"" . time() . "\" )",
			"select * from addrbook" ) );

if( 0 == $respObj->getErrorCode() ) {
	$rsCount = $respObj->getResultCount();

	for( $i = 0; $i < $rsCount; $i++ ) {
		$rs = $respObj->getResultSet( $i );
		printResultSet( $rs );
	}
} else {
	echo $respObj->getErrdataCode() . ": " . $respObj->getErrdataMsg();
}

?>

