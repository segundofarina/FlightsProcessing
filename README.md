# TP-POD

## Build

This project is developed on Java and using Maven, for building you need  _Maven +3.5.0_ and
_Java SE 8_ installed. Then, run:

```
$ mvn clean install
```


## Execution
For executing the server run:
```
$ ./runServer.sh
```

For executing the client run:
```
$ ./runClient.sh
```

## Parameters
The client script can be parametrized with the following parameters:
* **_-Daddresses:_** IP addresses of nodes in the cluster <br>
* **_-Dquery:_** Number of query to run<br>
* **_-DmovementsInPath:_** CSV file containing the movements of the airports<br>
* **_-DairportsInPath:_** CSV file containing the airports <br>
* **_-DoutPath:_** outPath where the results of query are output<br>
* **_-DtimeOutPath:_** outPath where time statistics are output<br>

For example:
```
$ ./runClient -Daddresses=10.6.0.1;10.6.0.2 -Dquery=5 -DmovementsInPath=movimientos.csv 
-DairportsInPath=aeropuertos.csv -DoutPath=query5.csv -DtimeOutPath=query5.txt -Dn=5
```

