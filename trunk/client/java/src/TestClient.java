/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

package sphivedbcli;

import java.util.Random;
import java.util.ArrayList;

public class TestClient {

	protected static void printResultSet( SPHiveResultSet rs ) {
		System.out.println( "column " + rs.getColumnCount() + ", row " + rs.getRowCount() );

		for( int i = 0; i < rs.getColumnCount(); i++ ) {
			System.out.print( "\t" + rs.getName( i ) + "(" + rs.getType( i ) + ")" );
		}
		System.out.println();

		for( int i = 0; i < rs.getRowCount(); i++ ) {
			rs.moveTo( i );
			for( int j = 0; j < rs.getColumnCount(); j++ ) {
				System.out.print( "\t" + rs.getString( j ) + "\t" );
			}
			System.out.println();
		}

		System.out.println();
	}

	public static void main( String [] args ) {
		if( 1 != args.length ) {
			System.out.println( "Usage: TestClient <config file>" );
			return;
		}

		String configFile = args[0];

		SPHiveDBClient client = new SPHiveDBClient();
		client.init( configFile );

		int dbfile = 0;
		String user = "foobar";
		String dbname = "addrbook";

		String sql [] = { "select * from addrbook" };

		SPHiveRespObject respObj = client.execute( dbfile, user, dbname, sql );

		if( 0 == respObj.getErrorCode() ) {
			System.out.println( "result " + respObj.getResultCount() );

			for( int i = 0; i < respObj.getResultCount(); i++ ) {
				SPHiveResultSet rs = respObj.getResultSet( i );
				printResultSet( rs );
			}
		} else {
			System.out.println( "errcode " + respObj.getErrdataCode() + ", errmsg " + respObj.getErrdataMsg() );
		}
	}

};

