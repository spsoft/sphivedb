# Introduction #

SPHiveDB uses JSONRPC over HTTP, this page describes the request/response message format.


# Details #

The request message:

```
{  
        "method" : "execute",  
        "params" : [  
                {  
                        "dbfile" : 0,  
                        "user" : "foobar",  
                        "dbname" : "addrbook",  
                        "sql" : [  
                                "insert into addrbook values ( 1, \"foo@bar.com\" )",  
                                "select * from addrbook"  
                        ]  
                }  
        ],  
        "id" : "foobar"  
}
```

The response message:
```
{  
        "result" : [  
                {  
                        "name" : [ "affected", "last_insert_rowid" ],  
                        "type" : [ "int", "int" ],  
                        "row" : [ [ 1, 1 ] ]  
                },  
                {  
                        "name" : [ "id", "addr" ]  
                        "type" : [ "int", "varchar(64)" ],  
                        "row" : [ [ "1", "foo@bar.com" ] ],  
                }  
        ],  
        "id" : "foobar"  
}  
```