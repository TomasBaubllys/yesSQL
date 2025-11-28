# yesSQL  
  
yesSQL is an open source database project born out of hatred for javascript. yesSQL is a non-relational (noSQL) key - value database, internally based on the LSM-Tree structure and partitioning.

For now it is mainly supported on Linux, to run it on windows, you will have to compose the docker containers yourself.
    
## How to run  
For now (and for the foreseeable future) the database is containerized using Docker. To launch the primary database server and all the partitions, make sure you have Docker installed and from the base directory (yesSQL folder) run:  
```
bash docker/ysql_server/compose.sh
```

Please, note that the bare servers use a custom binary protocol. You can checkout the ![binary protocol documentation](documentation/server_protocol) to know how to speak it. 

The primary server will have the port 9000 exposed by default, which you can change, in the docker-compose.yml file.

## Launching the CLI client
The current CLI client is written in the GO programming language. To launch it simply run:
```
go run cli.go
```
## Supported operations
By default yesSQL database supports these commands:
Set is pretty self explanatory it inserts the key - value pair into the dabatase.
```
SET <key> <value>
```
Get returns the key - value pair that matches the given key.
```
GET <key>
```
Removes the key - value pair from the database. Keep in mind even if the pair doesnt exist, the remove will still just mark it as removed and return OK.
```
REMOVE <key>
```
Creates the cursor for the current client, if key is not provided, the cursor will point to the beginning of the database 
```
CREATE_CURSOR <cursor_name> [key] 
```
Deletes the cursor for this client
```
DELETE_CURSOR <cursor_name>
```
Returns amount (amount <= 255) of key - value pairs forward from the current cursor.
```
GET_FF <cursor_name> <amount>
```
Returns amount (amount <= 255) of key - value pairs backwards from the current cursor.
```
GET_FB <cursor_name> <amount>
```
Returns amount (amount <= 255) of keys  forward from the current cursor.
```
GET_KEYS <cursor_name> <amount>
```
Returns amount (amount <= 255) of keys  forward from the current cursor that have the provided prefix.
```
GET_KEYS_PREFIX <cursor_name> <amount> <prefix>
```

## HTTP Bridge
Of course nobody likes custom binary protocols, so you can use the provided HTTP bridge server (of course it is also containerized using Docker). You can run it using:
```
bash docker/http_bridge/compose.sh
```
The HTTP bridge allows you to write applications, without having to care about custom protocols, to understand how to format the requests, see ![http request documentation](documentation/http_bridge).

The HTTP bridge will run on port 8000 by default.

## Customizing your own yesSQL server
If you wish to change the amount of partitions, or the amount of worker threads inside the thread pool, you can modify the provided ![database_setup.env](docker/ysql_server/database_setup.env) file. The values are pretty self explanitory:
```
// change the number 8 to any number of partitions you like (recommended up to 64)
PARTITION_COUNT=8

// change this line to change the number of worker threads inside the primary server (recommended up to 64)
PRIMARY_SERVER_THREAD_POOL_SIZE=32

// change this line to change the number of worker threads inside the partition servers (recommended up to 128)
PARTITION_SERVER_THREAD_POOL_SIZE=32
```

## Launching the example application
If you for some reason wish to launch the example application provided by us, simply run:
```
bash docker/example_app/compose.sh
```
and (maybe) enjoy:). You can access it on your browser http://localhost:8080
## Note by developers
This project was made out of curiosity on database internally work, if you wish to contribute feel free to make a pull request.



