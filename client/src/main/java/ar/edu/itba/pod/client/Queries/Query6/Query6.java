package ar.edu.itba.pod.client.Queries.Query6;

import ar.edu.itba.pod.Airport;
import ar.edu.itba.pod.Movement;
import ar.edu.itba.pod.Query6.CitiesMovementsCombinerFactory;
import ar.edu.itba.pod.Query6.CitiesMovementsMapper;
import ar.edu.itba.pod.Query6.CitiesMovementsReducerFactory;
import ar.edu.itba.pod.Query6.CitiesTuple;
import ar.edu.itba.pod.client.Printer;
import ar.edu.itba.pod.client.Queries.Query;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class Query6 implements Query {
    private final IList<Movement> movements;
    private final IList<Airport> airports;
    private final HazelcastInstance hz;
    private final int minMovements;
    private final Printer printer;

    public Query6(IList<Movement> movements, IList<Airport> airports, HazelcastInstance hz, int minMovements, Printer printer) {
        this.movements = movements;
        this.airports = airports;
        this.hz = hz;
        this.minMovements = minMovements;
        this.printer = printer;
    }

    /*
     * Generate oaci -> city map.
     * Create map reduce job iterates over movements list
     * and gets (oaci -> city map) by constructor,
     * emit citiesTuple -> 1 if cities are diferent,
     * reducer sums movements.
     * */

    @Override
    public void run() throws InterruptedException, ExecutionException {
        /* Create Query 6 Job */
        JobTracker jobTracker = hz.getJobTracker("Query6");

        /* Key is Oaci, Value is City */
        Map<String, String> oaciCityMap = getOaciCityMap(airports);

        /* Get cities movements
         * Key is citiesTuple, Value is amount of movements */
        Map<CitiesTuple, Integer> citiesMovements = getCitiesMovements(jobTracker, movements, oaciCityMap);

        /* Get query output */
        List<QueryOutputRow> queryOutput = getQueryOutput(citiesMovements);

        /* Print query output */
        printOutput(queryOutput);
    }

    private Map<String, String> getOaciCityMap(List<Airport> airports) {
        Map<String, String> oaciCityMap = new HashMap<>();

        for(Airport airport : airports) {
            airport.getOaci().ifPresent(oaci -> oaciCityMap.put(oaci, airport.getCity()));
        }

        return oaciCityMap;
    }

    private Map<CitiesTuple, Integer> getCitiesMovements(JobTracker jobTracker, IList<Movement> hzMovements, Map<String, String> oaciCityMap) throws InterruptedException, ExecutionException {
        /* Key is collection name */
        KeyValueSource<String, Movement> source = KeyValueSource.fromList(hzMovements);
        Job<String, Movement> job = jobTracker.newJob(source);

        /* Run map reduce */
        ICompletableFuture<Map<CitiesTuple, Integer>> future = job
                .mapper(new CitiesMovementsMapper(oaciCityMap))
                .combiner(new CitiesMovementsCombinerFactory())
                .reducer(new CitiesMovementsReducerFactory())
                .submit();

        /* Get map reduce output */
        return future.get();
    }

    private List<QueryOutputRow> getQueryOutput(Map<CitiesTuple, Integer> citiesMovements) {
        List<QueryOutputRow> queryOutput = new ArrayList<>();

        for(CitiesTuple citiesTuple : citiesMovements.keySet()) {
            int movements = citiesMovements.get(citiesTuple);

            if(minMovements <= movements) {
                String cityA = citiesTuple.getCity1();
                String cityB = citiesTuple.getCity2();

                if(citiesTuple.getCity1().compareTo(citiesTuple.getCity2()) > 0) {
                    cityA = citiesTuple.getCity1();
                    cityB = citiesTuple.getCity2();
                }

                queryOutput.add(new QueryOutputRow(cityA, cityB, citiesMovements.get(citiesTuple)));
            }
        }

        Collections.sort(queryOutput);

        return queryOutput;
    }

    private void printOutput(List<QueryOutputRow> queryOutput) {
        printer.appendToFile("Provincia A;Provincia B;Movimeintos\n");

        for(QueryOutputRow row : queryOutput) {
            printer.appendToFile(row+"\n");
        }
    }

    private class QueryOutputRow implements Comparable<QueryOutputRow> {
        private final String cityA;
        private final String cityB;
        private final int movements;

        public QueryOutputRow(String cityA, String cityB, int movements) {
            this.cityA = cityA;
            this.cityB = cityB;
            this.movements = movements;
        }

        @Override
        public String toString() {
            return cityA + ";" + cityB + ";" + movements;
        }

        @Override
        public int compareTo(QueryOutputRow o) {
            return o.movements - movements;
        }
    }
}
