package ar.edu.itba.pod.client.Queries.Query5;

import ar.edu.itba.pod.Airport;
import ar.edu.itba.pod.Movement;
import ar.edu.itba.pod.Query5.InternationalCombinerFactory;
import ar.edu.itba.pod.Query5.InternationalMapper;
import ar.edu.itba.pod.Query5.InternationalReducerFactory;
import ar.edu.itba.pod.client.Printer;
import ar.edu.itba.pod.client.Queries.Query;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Query5 implements Query {
    private final IList<Movement> movements;
    private final IList<Airport> airports;
    private final HazelcastInstance hz;
    private final Printer printer;
    private final int numberOfResults;

    public Query5(IList<Movement> movements, IList<Airport> airports, HazelcastInstance hz, int n, Printer printer) {
        this.movements = movements;
        this.airports = airports;
        this.hz = hz;
        this.printer = printer;
        this.numberOfResults = n;
    }

    @Override
    public void run() throws InterruptedException, ExecutionException {
        /* Create Query 5 Job */
        JobTracker jobTracker = hz.getJobTracker("Query5");

        /* Generate oaci->iata map */
        Map<String, String> oaciIataMap = getOaciIataMap(airports);

        /* Get movements group amount
         * Key is iata code, Value is percentage of international flights */
        Map<String, Integer> internationalFlights = getInternationalFlights(jobTracker, movements, oaciIataMap);

        /* Get query output */
        List<QueryOutputRow> queryOutput = getQueryOutput(internationalFlights);

        /* Print query output */
        printOutput(queryOutput);
    }

    private Map<String, String> getOaciIataMap(List<Airport> airports) {
        /* Key is oaci, value is iata */
        Map<String, String> oaciIataMap = new HashMap<>();

//        for(Airport airport : airports) {
//            if(airport.getOaci() != null && airport.getIata() != null) {
//                oaciIataMap.put(airport.getOaci(), airport.getIata());
//            }
//        }
        for(Airport airport : airports) {
            airport.getOaci().ifPresent(oaci -> airport.getIata().ifPresent(iata-> oaciIataMap.put(oaci, iata)));
        }

        return oaciIataMap;
    }

    private Map<String, Integer> getInternationalFlights(JobTracker jobTracker, IList<Movement> hzMovement, Map<String, String> oaciIataMap)
            throws InterruptedException, ExecutionException {
        /* Key is collection name */
        KeyValueSource<String, Movement> source = KeyValueSource.fromList(hzMovement);
        Job<String, Movement> job = jobTracker.newJob(source);

        /* Run map reduce
         * Key is iata code, value is percentage of international flights */
        ICompletableFuture<Map<String, Integer>> future = job
                .mapper(new InternationalMapper(oaciIataMap))
                .combiner(new InternationalCombinerFactory())
                .reducer(new InternationalReducerFactory())
                .submit();

        /* Get map reduce output */
        return future.get();
    }

    private List<QueryOutputRow> getQueryOutput(Map<String, Integer> internationalFlights) {
        List<QueryOutputRow> queryOutput = new ArrayList<>();

        for(String iata : internationalFlights.keySet()) {
            queryOutput.add(new QueryOutputRow(iata, internationalFlights.get(iata)));
        }

        queryOutput.sort((QueryOutputRow o1, QueryOutputRow o2) -> {
            int intFlightsPercentageCmp = o2.percentage - o1.percentage;
            if(intFlightsPercentageCmp == 0) {
                return o1.iata.compareTo(o2.iata);
            }
            return intFlightsPercentageCmp;
        });

        return queryOutput;
    }

    private void printOutput(List<QueryOutputRow> queryOutput) {
        System.out.println("IATA;Porcentaje");
        printer.appendToFile("IATA;Porcentaje\n");

        for(int i = 0; i < numberOfResults && i< queryOutput.size(); i++) {
            System.out.println(queryOutput.get(i));
            printer.appendToFile(queryOutput.get(i)+"\n");
        }
    }

    private class QueryOutputRow {
        private final String iata;
        private final int percentage;

        public QueryOutputRow(String iata, int percentage) {
            this.iata = iata;
            this.percentage = percentage;
        }

        @Override
        public String toString() {
            return iata + ";" + percentage + "%";
        }
    }
}
