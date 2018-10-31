package ar.edu.itba.pod.client;

import ar.edu.itba.pod.Airport;
import ar.edu.itba.pod.Movement;
import ar.edu.itba.pod.query1.MovementReducerFactory;
import ar.edu.itba.pod.query1.MovementsMapper;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Client {
    private static Logger logger = LoggerFactory.getLogger(Client.class);

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        logger.info("tp Client Starting ...");

        // Load params

        // Load csv to list
        CsvParser<Airport> airportCsvParser = new AirportParser();
        List<Airport> airports = airportCsvParser.loadFile(Paths.get("aeropuertos.csv"));

        //System.out.println(airports);
        //System.out.println("done airports");

        CsvParser<Movement> movementCsvParser = new MovementParser();
        List<Movement> movements = movementCsvParser.loadFile(Paths.get("movimientos.csv"));

        //System.out.println(movements);
        //System.out.println("done movements");

        /* Connect client to hazelcast */
        HazelcastInstance hz = HazelcastClient.newHazelcastClient();

        // Add list to hz
        IList<Airport> hzAirports = hz.getList("airports");
        hzAirports.addAll(airports);

        IList<Movement> hzMovement = hz.getList("movements");
        hzMovement.addAll(movements);

        // Create new map-reduce job
        JobTracker jobTracker = hz.getJobTracker("query1");
        KeyValueSource<String, Movement> source = KeyValueSource.fromList(hzMovement);

        Job<String, Movement> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, Integer>> future = job
                .mapper(new MovementsMapper())
                .reducer(new MovementReducerFactory())
                .submit();

        Map<String, Integer> result = future.get();

        System.out.println("done");
        System.out.println(result);

        // Shutdown this Hazelcast client
        hz.shutdown();
    }
}
