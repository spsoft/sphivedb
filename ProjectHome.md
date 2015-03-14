SPHiveDB is a server for sqlite database. It use JSON-RPC over HTTP to expose a network interface to use SQLite database. It supports combining multiple SQLite databases into one file through Tokyo Cabinet. It also supports the use of multiple files. It is designed for the extreme sharding schema -- one SQLite database per user.

![http://lh4.ggpht.com/_ujFk6cBIKlI/SjT1OS--ARI/AAAAAAAAAJQ/rz7tE60kbYY/sphivedb.jpg](http://lh4.ggpht.com/_ujFk6cBIKlI/SjT1OS--ARI/AAAAAAAAAJQ/rz7tE60kbYY/sphivedb.jpg)

```
Changelog:

Changes in version 0.7.5 (12.06.2009)
-------------------------------------
* PHP client api was added
* Upgrade sqlite to 3.6.20

Changes in version 0.7 (10.18.2009)
-------------------------------------
* Replace json by protobuf's wire format to improve performance

Changes in version 0.6 (07.11.2009)
-------------------------------------
* Python client api was added
* Remove function was added

Changes in version 0.5 (07.04.2009)
-------------------------------------
* C++/java client api were added
* Stress test tools was added
* A lock fail problem was fixed

Changes in version 0.4 (06.27.2009)
-------------------------------------
* Support multiple tables
* Report more detailed error messages

Changes in version 0.3 (06.21.2009)
-------------------------------------
* SPSqliteSvr was added，support one sqlite database one file schema

Changes in version 0.2 (06.14.2009)
-------------------------------------
* Automatic synchronization of database structure depending on a create table statement is supported. 
* Subdirectories were added to contain more database files.

Changes in version 0.1 (05.23.2009)
-------------------------------------
* version 0.1 release

```