package ar.edu.itba.pod.client.Queries.Query2;

import ar.edu.itba.pod.Movement;
import ar.edu.itba.pod.Query1.MovementsCombinerFactory;
import ar.edu.itba.pod.Query1.MovementsMapper;
import ar.edu.itba.pod.Query1.MovementsReducerFactory;
import ar.edu.itba.pod.Query2.ThousandMovementsMapper;
import ar.edu.itba.pod.Query2.ThousandMovementsReducerFactory;
import ar.edu.itba.pod.client.Printer;
import ar.edu.itba.pod.client.Queries.Query;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class Query2 implements Query {
    private IList<Movement> movements;
    private HazelcastInstance hz;
    private Printer printer;

    public Query2(IList<Movement> movements, HazelcastInstance hz,Printer printer) {
        this.movements = movements;
        this.hz = hz;
        this.printer = printer;
    }

    @Override
    public void run() throws InterruptedException, ExecutionException {
        /* Create Query 2 Job */
        JobTracker jobTracker = hz.getJobTracker("Query2");

        /* Oaci movements map, Key is oaci, value is amount of movements */
        Map<String, Integer> oaciMovementsMap = getOaciMovmentsMap(jobTracker, movements);

        /* Get movements group amount
         * Key is thousand of movements, Value is a list of oaci codes */
        Map<Integer, List<String>> thousandOfMovements = getThousandOfMovements(jobTracker, oaciMovementsMap);

        /* Get query output */
        List<QueryOutputRow> queryOutput = getQueryOutput(thousandOfMovements);

        /* Print query output */
        printOutput(queryOutput);
    }

    private Map<String, Integer> getOaciMovmentsMap(JobTracker jobTracker, IList<Movement> hzMovement) throws InterruptedException, ExecutionException {
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
        return future.get();
    }

    private Map<Integer, List<String>> getThousandOfMovements(JobTracker jobTracker, Map<String, Integer> oaciMovementsMap) throws InterruptedException, ExecutionException {
        /* Load oaci movements map to hz */
        IMap<String, Integer> hzOaciMovemntsMap = hz.getMap("oaciMovements");
        hzOaciMovemntsMap.putAll(oaciMovementsMap);

        /* Key is oaci, Value is amount of movements */
        KeyValueSource<String, Integer> source = KeyValueSource.fromMap(hzOaciMovemntsMap);
        Job<String, Integer> job = jobTracker.newJob(source);

        /* Run map reduce
        * Key is thousand of movements, Value is list of oaci codes */
        ICompletableFuture<Map<Integer, List<String>>> future = job
                .mapper(new ThousandMovementsMapper())
                .reducer(new ThousandMovementsReducerFactory())
                .submit();

        /* Get map reduce output
         * Key is thousand of movements, Value is a list of oaci codes */
        Map<Integer, List<String>> thousandOfMovements = future.get();

        /* Remove oaciMovements Map from Hazelcast */
        hzOaciMovemntsMap.destroy();

        return thousandOfMovements;
    }

    private List<QueryOutputRow> getQueryOutput(Map<Integer, List<String>> thosandOfMovements) {
        List<QueryOutputRow> queryOutput = new ArrayList<>();

        for(Integer movementAmount : thosandOfMovements.keySet()) {
            List<String> values = thosandOfMovements.get(movementAmount);

            if(values.size() > 1) {
                Collections.sort(values);

                for(int i = 0; i < values.size(); i++) {
                    String oaci1 = values.get(i);

                    for(int j = i + 1; j < values.size(); j++) {
                        String oaci2 = values.get(j);
                        queryOutput.add(new QueryOutputRow(movementAmount, oaci1, oaci2));
                    }
                }
            }
        }

        /* Sort query output */
        Collections.sort(queryOutput);

        return queryOutput;
    }

    private void printOutput(List<QueryOutputRow> queryOutput) {
        printer.appendToFile("Grupo;Aeropuerto 1;Aeropuerto 2\n");
        for(QueryOutputRow row : queryOutput) {
            printer.appendToFile(row+"\n");
        }
    }

    private class QueryOutputRow implements Comparable<QueryOutputRow> {
        private final int thousandMovements;
        private final String oaci1;
        private final String oaci2;

        public QueryOutputRow(int thousandMovements, String oaci1, String oaci2) {
            this.thousandMovements = thousandMovements;
            this.oaci1 = oaci1;
            this.oaci2 = oaci2;
        }

        @Override
        public String toString() {
            return thousandMovements + ";" + oaci1 + ";" + oaci2;
        }

        @Override
        public int compareTo(QueryOutputRow o) {
            return o.thousandMovements - thousandMovements;
        }
    }
}
