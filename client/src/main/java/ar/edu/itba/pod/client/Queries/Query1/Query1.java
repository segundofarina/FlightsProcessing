package ar.edu.itba.pod.client.Queries.Query1;

import ar.edu.itba.pod.Airport;
import ar.edu.itba.pod.Movement;
import ar.edu.itba.pod.Query1.MovementsCombinerFactory;
import ar.edu.itba.pod.Query1.MovementsReducerFactory;
import ar.edu.itba.pod.Query1.MovementsMapper;
import ar.edu.itba.pod.client.Queries.Query;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class Query1 implements Query {
    private List<Airport> airports;
    private List<Movement> movements;
    private HazelcastInstance hz;

    public Query1(List<Airport> airports, List<Movement> movements, HazelcastInstance hz) {
        this.airports = airports;
        this.movements = movements;
        this.hz = hz;
    }

    @Override
    public void run() throws InterruptedException, ExecutionException {
        /* Add movements list to hazelcast */
        IList<Movement> hzMovement = hz.getList("movements");
        hzMovement.addAll(movements);

        /* Create Query 1 Job */
        JobTracker jobTracker = hz.getJobTracker("Query1");

        /* Key is collection name */
        KeyValueSource<String, Movement> source = KeyValueSource.fromList(hzMovement);
        Job<String, Movement> job = jobTracker.newJob(source);

        /* Run map reduce
         * Key is OACI, Value is number of movements */
        ICompletableFuture<Map<String, Integer>> future = job
                .mapper(new MovementsMapper())
                .combiner(new MovementsCombinerFactory())
                .reducer(new MovementsReducerFactory())
                .submit();

        /* Get map reduce output */
        Map<String, Integer> oaciMovementsMap = future.get();

        /* Remove Movements Map from Hazelcast */
        hzMovement.destroy();

        /* Get complete and sorted output */
        List<QueryOutputRow> queryOutput = generateQueryOutput(oaciMovementsMap);

        printOutput(queryOutput);
    }

    private List<QueryOutputRow> generateQueryOutput(Map<String, Integer> oaciMovemntsMap) {
        List<QueryOutputRow> queryOutput = new ArrayList<>();

        Map<String, String> oaciNameMap = getOaciNameMap();

        /* Generate output joining oaci with name */
        for(String oaci : oaciMovemntsMap.keySet()) {
            queryOutput.add(new QueryOutputRow(oaci, oaciNameMap.get(oaci), oaciMovemntsMap.get(oaci)));
        }

        /* Sort output list by movements amount decendent, and OACI */
        queryOutput.sort((QueryOutputRow o1, QueryOutputRow o2) -> {
            if(o1.sum == o2.sum) {
                return o1.OACI.compareTo(o2.OACI);
            }
            return o2.sum - o1.sum;
        });

        return queryOutput;
    }

    private void printOutput(List<QueryOutputRow> queryOutput) { // THIS SHOULD PRINT TO EXTERNAL FILE
        System.out.println("OACI;Denominacion;Movimientos");

        for(QueryOutputRow row : queryOutput) {
            System.out.println(row);
        }
    }

    private Map<String, String> getOaciNameMap() {
        Map<String, String> oaciName = new HashMap<>();

        for(Airport airport : airports) {
            if(airport.getOaci() != null) { // THIS SHOULD BE OPTIONAL
                oaciName.put(airport.getOaci(), airport.getName());
            }
        }

        return oaciName;
    }

    private class QueryOutputRow {
        private final String OACI;
        private final String name;
        private final int sum;

        public QueryOutputRow(String OACI, String name, int sum) {
            this.OACI = OACI;
            this.name = name;
            this.sum = sum;
        }

        @Override
        public String toString() {
            String notNullName = name == null ? "" : name;
            return OACI + ";" + notNullName + ";" + sum;
        }
    }
}
