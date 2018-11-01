package ar.edu.itba.pod.client.Queries.Query5;

import ar.edu.itba.pod.Movement;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Query5 implements Query {
    private final List<Movement> movements;
    private final HazelcastInstance hz;
    private final Printer printer;
    private final int numberOfResults;

    public Query5(List<Movement> movements, HazelcastInstance hz,int n, Printer printer) {
        this.movements = movements;
        this.hz = hz;
        this.printer = printer;
        this.numberOfResults=n;
    }


    @Override
    public void run() throws InterruptedException, ExecutionException {
        /* Add movements list to hazelcast */
        IList<Movement> hzMovement = hz.getList("movements");
        hzMovement.addAll(movements);

        /* Create Query 4 Job */
        JobTracker jobTracker = hz.getJobTracker("Query5");

        /* Get movements group amount
         * Key is oaci origin, Value is amount of landings */
        Map<String, Integer> landingsAmount = getLandingsAmount(jobTracker, hzMovement);

        /* Get query output */
        List<QueryOutputRow> queryOutput = getQueryOutput(landingsAmount);

        /* Print query output */
        printOutput(queryOutput);
    }

    private Map<String, Integer> getLandingsAmount(JobTracker jobTracker, IList<Movement> hzMovement)
            throws InterruptedException, ExecutionException {
        /* Key is collection name */
        KeyValueSource<String, Movement> source = KeyValueSource.fromList(hzMovement);
        Job<String, Movement> job = jobTracker.newJob(source);

        /* Run map reduce */
        ICompletableFuture<Map<String, Integer>> future = job
                .mapper(new InternationalMapper())
                //.combiner(new LandingsAmountCombinerFactory())
                .reducer(new InternationalReducerFactory())
                .submit();

        /* Get map reduce output */
        return future.get();
    }

    private List<QueryOutputRow> getQueryOutput(Map<String, Integer> landingsAmount) {
        List<QueryOutputRow> queryOutput = new ArrayList<>();

        for(String oaci : landingsAmount.keySet()) {
            queryOutput.add(new QueryOutputRow(oaci, landingsAmount.get(oaci)));
        }

        queryOutput.sort((QueryOutputRow o1, QueryOutputRow o2) -> {
            int landingsAmountCmp = o2.percentage - o1.percentage;
            if(landingsAmountCmp == 0) {
                return o1.iata.compareTo(o2.iata);
            }
            return landingsAmountCmp;
        });

        return queryOutput;
    }

    private void printOutput(List<QueryOutputRow> queryOutput) {
        System.out.println("IATA;Porcentaje");
        printer.appendToFile("IATA;Porcentaje\n");

        for(int i = 0; i < numberOfResults; i++) {
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
            return iata + ";" + percentage;
        }
    }
}
