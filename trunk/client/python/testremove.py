
import sphivedbcli
import time
import sys

if __name__ == "__main__":
	if len( sys.argv ) != 2:
		print "Usage: %s <config file>" % ( sys.argv[0] )
		print "\tpython %s ../../sphivedb/sphivedbcli.ini" % ( sys.argv[0] )
		sys.exit( -1 )

	configFile = sys.argv[1]

	cli = sphivedbcli.SPHiveDBClient()

	cli.init( configFile )

	try:
		resp = cli.remove( 0, "foobar", "addrbook" )

		print resp

	except Exception, e:
		print e

