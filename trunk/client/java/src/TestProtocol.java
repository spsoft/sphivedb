/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

package sphivedbcli;

import java.net.URL;

public class TestProtocol {
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

	public static void main( String [] args ) throws Exception {
		if( 5 != args.length ) {
			System.out.println( "Usage: TestProtocol <url> <dbfile> <user> <dbname> <sql>" );
			return;
		}

		String url = args[0];
		int dbfile = Integer.parseInt( args[1] );
		String user = args[2];
		String dbname = args[3];
		String sql[] = args[4].split( ";" );

		SPHiveDBProtocol protocol = new SPHiveDBProtocol( url );

		SPHiveRespObject respObj = protocol.execute( dbfile, user, dbname, sql );

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

