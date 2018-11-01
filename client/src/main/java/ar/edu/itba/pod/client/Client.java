package ar.edu.itba.pod.client;

import ar.edu.itba.pod.Airport;
import ar.edu.itba.pod.Movement;
import ar.edu.itba.pod.client.Parsers.AirportParser;
import ar.edu.itba.pod.client.Parsers.CsvParser;
import ar.edu.itba.pod.client.Parsers.MovementParser;
import ar.edu.itba.pod.client.Queries.Query;
import ar.edu.itba.pod.client.Queries.Query1.Query1;
import ar.edu.itba.pod.client.Queries.Query2.Query2;
import ar.edu.itba.pod.client.Queries.Query3.Query3;
import ar.edu.itba.pod.client.Queries.Query4.Query4;
import ar.edu.itba.pod.client.Queries.Query5.Query5;
import ar.edu.itba.pod.client.Queries.Query6.Query6;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class Client {
    private static Logger logger = LoggerFactory.getLogger(Client.class);


    public static void main(String[] args) throws InterruptedException, ExecutionException {
        logger.info("tp Client Starting ...");
        
        ParamLoader params = new ParamLoader();

        logParmsLoaded(params);

        Printer times = new Printer(params.getTimeOutPath());
        Printer out =  new Printer(params.getOutPath());
        /* Load csv to list */
        times.log("Inicio de lectura del archivo");
        CsvParser<Airport> airportCsvParser = new AirportParser();
        List<Airport> airports = airportCsvParser.loadFile(Paths.get(params.getAirportsInPath()));

        CsvParser<Movement> movementCsvParser = new MovementParser();
        List<Movement> movements = movementCsvParser.loadFile(Paths.get(params.getMovementsInPath()));

        times.log("Fin de lectura del archivo");

        /* Connect client to hazelcast */
        HazelcastInstance hz = HazelcastClient.newHazelcastClient();

        /* Get Query */
        Query query = getQuery(params.getQueryNumber(), out, airports, movements, hz);


        /* Run Query */
        times.log("Inicio del trabajo Map/reduce");
        query.run();
        times.log("Fin del trabajo Map/reduce");

        /* Shutdown this Hazelcast client */
        hz.shutdown();
        times.close();
        out.close();

    }

    private static void logParmsLoaded(ParamLoader params) {
        logger.info("query: " + params.getQueryNumber());
        logger.info("movementsInPath: " + params.getMovementsInPath());
        logger.info("airportsInPath: " + params.getAirportsInPath());
        logger.info("outPath: " + params.getOutPath());
        logger.info("timeOutPath: " + params.getTimeOutPath());
    }


    private static Query getQuery(int queryNumber, Printer p, List<Airport> airports, List<Movement> movements, HazelcastInstance hz){
        Query query;
        int n,min;

        switch (queryNumber){
            case 1:
                query = new Query1(airports, movements, hz, p);
                break ;
            case 2:
                query = new Query2(movements, hz, p);
                break;

            case 3:
                query = new Query3(movements, hz, p);
                break;
            case 4:
                String oaci = Optional.ofNullable(System.getProperty("oaci")).orElseThrow(IllegalArgumentException::new);
                n = Integer.parseInt(Optional.ofNullable(System.getProperty("n")).orElseThrow(IllegalArgumentException::new));
                query = new Query4(movements, hz, oaci, n, p);
                break;
            case 5:
                n = Integer.parseInt(Optional.ofNullable(System.getProperty("n")).orElseThrow(IllegalArgumentException::new));
                query = new Query5(movements, hz, n, p);
                break;
            case 6:
                min = Integer.parseInt(Optional.ofNullable(System.getProperty("min")).orElseThrow(IllegalArgumentException::new));
                query = new Query6(movements, airports, hz, min, p);
                break;
            default:
                throw new IllegalArgumentException("There is no query number " + queryNumber);
        }

        return query;
    }

}
