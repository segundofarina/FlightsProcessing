package ar.edu.itba.pod.client;

import ar.edu.itba.pod.Airport;
import ar.edu.itba.pod.Movement;
import ar.edu.itba.pod.client.Parsers.AirportParser;
import ar.edu.itba.pod.client.Parsers.CsvParser;
import ar.edu.itba.pod.client.Parsers.MovementParser;
import ar.edu.itba.pod.client.Queries.Query;
import ar.edu.itba.pod.client.Queries.Query1.Query1;
import ar.edu.itba.pod.Query1.MovementReducerFactory;
import ar.edu.itba.pod.Query1.MovementsMapper;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class Client {
    private static Logger logger = LoggerFactory.getLogger(Client.class);

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        logger.info("tp Client Starting ...");

        // Load params

        /* Load csv to list */
        CsvParser<Airport> airportCsvParser = new AirportParser();
        List<Airport> airports = airportCsvParser.loadFile(Paths.get("aeropuertos.csv"));

        CsvParser<Movement> movementCsvParser = new MovementParser();
        List<Movement> movements = movementCsvParser.loadFile(Paths.get("movimientos.csv"));

        /* Connect client to hazelcast */
        HazelcastInstance hz = HazelcastClient.newHazelcastClient();

        /* Get Query */
        Query query = new Query1(airports, movements, hz);

        /* Run Query */
        query.run();

        /* Shutdown this Hazelcast client */
        hz.shutdown();
    }
}
