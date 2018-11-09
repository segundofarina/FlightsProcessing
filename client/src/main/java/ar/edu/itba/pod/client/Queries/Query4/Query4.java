package ar.edu.itba.pod.client.Queries.Query4;

import ar.edu.itba.pod.Movement;
import ar.edu.itba.pod.Query4.LandingsAmountCombinerFactory;
import ar.edu.itba.pod.Query4.LandingsAmountMapper;
import ar.edu.itba.pod.Query4.LandingsAmountReducerFactory;
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

public class Query4 implements Query {
    private final IList<Movement> movements;
    private final HazelcastInstance hz;
    private final String destinationOaci;
    private final int numberOfResults;
    private final Printer printer;

    public Query4(IList<Movement> movements, HazelcastInstance hz, String destinationOaci, int numberOfResults, Printer printer) {
        this.movements = movements;
        this.hz = hz;
        this.destinationOaci = destinationOaci;
        this.numberOfResults = numberOfResults;
        this.printer = printer;
    }


    @Override
    public void run() throws InterruptedException, ExecutionException {
        /* Create Query 4 Job */
        JobTracker jobTracker = hz.getJobTracker("Query4");

        /* Get movements group amount
         * Key is oaci origin, Value is amount of landings */
        Map<String, Integer> landingsAmount = getLandingsAmount(jobTracker, movements);

        /* Get query output */
        List<QueryOutputRow> queryOutput = getQueryOutput(landingsAmount);

        /* Print query output */
        printOutput(queryOutput);
    }

    private Map<String, Integer> getLandingsAmount(JobTracker jobTracker, IList<Movement> hzMovement) throws InterruptedException, ExecutionException {
        /* Key is collection name */
        KeyValueSource<String, Movement> source = KeyValueSource.fromList(hzMovement);
        Job<String, Movement> job = jobTracker.newJob(source);

        /* Run map reduce */
        ICompletableFuture<Map<String, Integer>> future = job
                .mapper(new LandingsAmountMapper(destinationOaci))
                .combiner(new LandingsAmountCombinerFactory())
                .reducer(new LandingsAmountReducerFactory())
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
            int landingsAmountCmp = o2.landingsAmount - o1.landingsAmount;
            if(landingsAmountCmp == 0) {
                return o1.oaciOrigin.compareTo(o2.oaciOrigin);
            }
            return landingsAmountCmp;
        });

        return queryOutput;
    }

    private void printOutput(List<QueryOutputRow> queryOutput) {
        printer.appendToFile("Oaci;Aterrizajes\n");

        for(int i = 0; i < queryOutput.size() && i < numberOfResults; i++) {
            printer.appendToFile(queryOutput.get(i)+"\n");
        }
    }

    private class QueryOutputRow {
        private final String oaciOrigin;
        private final int landingsAmount;

        public QueryOutputRow(String oaciOrigin, int landingsAmount) {
            this.oaciOrigin = oaciOrigin;
            this.landingsAmount = landingsAmount;
        }

        @Override
        public String toString() {
            return oaciOrigin + ";" + landingsAmount;
        }
    }
}
