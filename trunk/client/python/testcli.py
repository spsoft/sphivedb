
import sphivedbcli
import time
import sys

def printResultSet( rs ):
	print "row.count %d" % ( rs.getRowCount() )

	columnCount = rs.getColumnCount()

	hdrs = ""

	for i in range( columnCount ):
		hdrs = hdrs + ( "\t%s(%s)" % ( rs.getName( i ), rs.getType( i ) ) )

	print hdrs

	for i in range( rs.getRowCount() ):
		rs.moveTo( i )

		row = ""

		for j in range( columnCount ):
			row = row + ( "\t[%s]" % ( rs.getString( j ) ) )

		print row

if __name__ == "__main__":
	if len( sys.argv ) != 2:
		print "Usage: %s <config file>" % ( sys.argv[0] )
		print "\tpython %s ../../sphivedb/sphivedbcli.ini" % ( sys.argv[0] )
		sys.exit( -1 )

	configFile = sys.argv[1]

	cli = sphivedbcli.SPHiveDBClient()

	cli.init( configFile )

	try:
		resp = cli.execute( 0, "foobar", "addrbook", \
			[ "insert into addrbook ( addr ) values ( \"%d\" )" % ( time.time() ), \
			"select * from addrbook" ] )

		if 0 == resp.getErrorCode():
			rsCount = resp.getResultCount()

			for i in range( rsCount ):
				rs = resp.getResultSet( i )
				printResultSet( rs )
		else:
			print "%d: %s" % ( resp.getErrdataCode(), resp.getErrdataMsg() )

	except Exception, e:
		print e

