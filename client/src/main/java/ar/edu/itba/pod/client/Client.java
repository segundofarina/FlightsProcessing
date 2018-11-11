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
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static java.lang.System.exit;

public class Client {
    private static Logger logger = LoggerFactory.getLogger(Client.class);


    public static void main(String[] args) throws InterruptedException, ExecutionException {
        logger.info("tp Client Starting ...");
        
        ParamLoader params = new ParamLoader();

        logParmsLoaded(params);

        Printer times = new Printer(params.getTimeOutPath());
        Printer out =  new Printer(params.getOutPath());

        /* Connect client to hazelcast */
        HazelcastInstance hz;

        /* Get cluster members */
        ClientConfig hzConfig = getHzConfig();

        try {
            hz = HazelcastClient.newHazelcastClient(hzConfig);
        } catch (IllegalStateException e) {
            System.out.println("Unable to connect to cluster");
            return;
        }

        /* Load csv to list */
        times.log("Inicio de lectura del archivo");

        IList<Airport> airportsHz = hz.getList("airports");

        CsvParser airportCsvParser = new AirportParser(airportsHz);
        airportCsvParser.loadFile(Paths.get(params.getAirportsInPath()));

        IList<Movement> movementsHz = hz.getList("movements");

        CsvParser movementCsvParser = new MovementParser(movementsHz);
        movementCsvParser.loadFile(Paths.get(params.getMovementsInPath()));

        times.log("Fin de lectura del archivo");

        /* Get Query */
        Query query = getQuery(params.getQueryNumber(), out, airportsHz, movementsHz, hz);

        /* Run Query */
        times.log("Inicio del trabajo Map/reduce");

        query.run();

        times.log("Fin del trabajo Map/reduce");

        /* Remove lists from hz */
        airportsHz.destroy();
        movementsHz.destroy();

        /* Shutdown this Hazelcast client */
        hz.shutdown();

        /* Close file printers */
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

    private static Query getQuery(int queryNumber, Printer p, IList<Airport> airports, IList<Movement> movements, HazelcastInstance hz){
        Query query = null;
        Optional<String> n;

        switch (queryNumber){
            case 1:
                query = new Query1(airports, movements, hz, p);
                break;
            case 2:
                query = new Query2(movements, hz, p);
                break;
            case 3:
                query = new Query3(movements, hz, p);
                break;
            case 4:
                Optional<String> oaci = Optional.ofNullable(System.getProperty("oaci"));
                n = Optional.ofNullable(System.getProperty("n"));
                if(!oaci.isPresent() || !n.isPresent()) {
                    System.out.println("\n\nMissing Parameters\nQuery 4 should be run with -Doaci=<OACI Code> -Dn=<Number of airports>\n");
                    exit(1);
                }

                query = new Query4(movements, hz, oaci.get(), Integer.parseInt(n.get()), p);
                break;
            case 5:
                n = Optional.ofNullable(System.getProperty("n"));
                if(!n.isPresent()) {
                    System.out.println("\n\nMissing Parameters\nQuery 5 should be run with -Dn=<Number of airports>\n");
                    exit(1);
                }

                query = new Query5(movements, airports,hz, Integer.parseInt(n.get()), p);
                break;
            case 6:
                Optional<String> min = Optional.ofNullable(System.getProperty("min"));
                if(!min.isPresent()) {
                    System.out.println("\n\nMissing Parameters\nQuery 6 should be run with -Dmin=<Minimum movements>\n");
                    exit(1);
                }

                query = new Query6(movements, airports, hz, Integer.parseInt(min.get()), p);
                break;
            default:
                System.out.println("\n\nError - Query number not supported. Queries should go from 1 to 6.\n");
                exit(1);
        }

        return query;
    }

    private static ClientConfig getHzConfig() {
        String addresses = Optional.ofNullable(System.getProperty("addresses")).orElseThrow(IllegalArgumentException::new);
        String addressList[] = addresses.split(";");

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setGroupConfig(new GroupConfig("56086-56015-56176", "56086-56015-56176"));

        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();

        for (String address : addressList) {
            networkConfig.addAddress(address);
        }

        return clientConfig;
    }

}
