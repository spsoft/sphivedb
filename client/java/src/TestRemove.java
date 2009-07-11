/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

package sphivedbcli;

import java.util.Random;
import java.util.ArrayList;

public class TestRemove {

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

		int ret = client.remove( dbfile, user, dbname );

		System.out.println( "remove " + ret );
	}
};

