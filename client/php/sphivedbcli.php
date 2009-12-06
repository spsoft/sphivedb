<?php

/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

class SPHiveResultSet {
	private $inner;
	private $rowIndex;

	public function __construct( $result ) {
		$this->inner = $result;
		$this->rowIndex = 0;
	}

	public function getColumnCount() {
		if( isset( $this->inner[ "name" ] ) ) {
			return count( $this->inner[ "name" ] );
		}

		return 0;
	}

	public function getType( $index ) {
		if( isset( $this->inner[ "type" ] ) ) {
			return $this->inner[ "type" ][ $index ];
		}

		return NULL;
	}

	public function getName( $index ) {
		if( isset( $this->inner[ "name" ] ) ) {
			return $this->inner[ "name" ][ $index ];
		}

		return NULL;
	}

	public function getRowCount() {
		if( isset( $this->inner[ "row" ] ) ) {
			return count( $this->inner[ "row" ] );
		}

		return 0;
	}

	public function moveTo( $index ) {
		$this->rowIndex = $index;
	}

	public function getField( $index ) {
		if( isset( $this->inner[ "row" ] ) ) {
			$row = $this->inner[ "row" ][ $this->rowIndex ];
			return $row[ $index ];
		}

		return NULL;
	}
};

class SPHiveRespObject {
	private $inner;

	public function __construct( $response ) {
		$this->inner = $response;
	}

	public function getErrorCode() {
		if( isset( $this->inner[ "error" ] ) ) {
			$error = $this->inner[ "error" ];
			if( isset( $error[ "code" ] ) ) {
				return $error[ "code" ];
			}
		}

		return 0;
	}

	public function getErrorMsg() {
		if( isset( $this->inner[ "error" ] ) ) {
			$error = $this->inner[ "error" ];
			if( isset( $error[ "message" ] ) ) {
				return $error[ "message" ];
			}
		}

		return NULL;
	}

	public function getErrdataCode() {
		if( isset( $this->inner[ "error" ] ) ) {
			$error = $this->inner[ "error" ];
			if( isset( $error[ "data" ] ) ) {
				$data = $error[ "data" ];
				if( isset( $data[ "code" ] ) ) {
					return $data[ "code" ];
				}
			}
		}

		return 0;
	}

	public function getErrdataMsg() {
		if( isset( $this->inner[ "error" ] ) ) {
			$error = $this->inner[ "error" ];
			if( isset( $error[ "data" ] ) ) {
				$data = $error[ "data" ];
				if( isset( $data[ "message" ] ) ) {
					return $data[ "message" ];
				}
			}
		}

		return NULL;
	}

	public function getResultSet( $index ) {
		if( isset( $this->inner[ "result" ] ) ) {
			return new SPHiveResultSet( $this->inner[ "result"][ $index ] );
		}

		return NULL;
	}

	public function getResultCount() {
		if( isset( $this->inner[ "result" ] ) ) {
			return count( $this->inner[ "result" ] );
		}

		return 0;
	}
};

class SPEndPoint {
	public $ip;
	public $port;
	public $keyMin;
	public $keyMax;
};

class SPEndPointTable {
	private $list;

	public function __construct() {
		$this->list = array();
	}

	public function getCount() {
		return count( $this->list );
	}

	public function getByKey( $key ) {
		foreach( $this->list as $k => $v ) {
			if( $k->keyMin <= $key and $key <= $k->keyMax ) {
				return $v;
			}
		}
		return NULL;
	}

	public function add( $ep ) {
		array_push( $this->list, $ep );

		return 0;
	}

	public function get( $index ) {
		return $this->list[ $index ];
	}
};


class SPHiveDBClient {
	private $endPointTable;

	public function __construct() {
		$this->endPointTable = new SPEndPointTable();
	}

	public function init( $configFile ) {
		$sections = parse_ini_file( $configFile, TRUE ); 

		$endPointCount = $sections[ "EndPointTable" ][ "EndPointCount" ];

		for( $i = 0; $i < $endPointCount; $i++ ) {
			$ep = new SPEndPoint();

			$ep->ip = $sections[ "EndPoint" . $i ] [ "ServerIP" ];
			$ep->port = $sections[ "EndPoint" . $i ] [ "ServerPort" ];
			$ep->keyMin = $sections[ "EndPoint" . $i ] [ "KeyMin" ];
			$ep->keyMax = $sections[ "EndPoint" . $i ] [ "KeyMax" ];

			$this->endPointTable->add( $ep );
		}
	}

	public function execute( $dbfile, $user, $dbname, $sql ) {
		$ep = $this->endPointTable->getByKey( $dbfile );

		if( NULL == ep ) {
			error_log( "Cannot find endpoint for dbfile " . $dbfile );
			return NULL;
		}

		$url = "http://" . $ep->ip . ":" . $ep->port . "/sphivedb";

		$request = "";
		{
			$params = array( array( "dbfile" => (int)$dbfile, "user" => $user,
					"dbname" => $dbname, "sql" => $sql ) );
			$request = array( "method" => "execute", "params" => $params, "id" => $user );
			$request = json_encode($request);
		}

		$opts = array ('http' => array ( 'method'  => 'POST',
				'header'  => 'Content-type: application/json', 'content' => $request));
		$context  = stream_context_create($opts);
		if( $fp = fopen( $url, 'r', false, $context ) ) {
			$response = '';
			while($row = fgets($fp)) {
				$response.= trim($row)."\n";
			}
			fclose( $fp );
			$response = json_decode($response,true);

			return new SPHiveRespObject( $response );
		} else {
			error_log( "Unable to connect to " . $url );
		}

		return NULL;
	}

	public function remove( $dbfile, $user, $dbname ) {
		$ep = $this->endPointTable->getByKey( $dbfile );

		if( NULL == ep ) {
			error_log( "Cannot find endpoint for dbfile " . $dbfile );
			return NULL;
		}

		$url = "http://" . $ep->ip . ":" . $ep->port . "/sphivedb";

		$request = "";
		{
			$params = array( array( "dbfile" => (int)$dbfile, "user" => $user,
					"dbname" => $dbname ) );
			$request = array( "method" => "remove", "params" => $params, "id" => $user );
			$request = json_encode($request);
		}

		$opts = array ('http' => array ( 'method'  => 'POST',
				'header'  => 'Content-type: application/json', 'content' => $request));
		$context  = stream_context_create($opts);
		if( $fp = fopen( $url, 'r', false, $context ) ) {
			$response = '';
			while($row = fgets($fp)) {
				$response.= trim($row)."\n";
			}
			fclose( $fp );
			$response = json_decode($response,true);

			return $response[ "result" ];
		} else {
			error_log( "Unable to connect to " . $url );
		}

		return -1;
	}
};

?>

