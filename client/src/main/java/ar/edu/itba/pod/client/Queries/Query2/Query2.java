package ar.edu.itba.pod.client.Queries.Query2;

import ar.edu.itba.pod.Movement;
import ar.edu.itba.pod.Query1.MovementsCombinerFactory;
import ar.edu.itba.pod.Query1.MovementsMapper;
import ar.edu.itba.pod.Query1.MovementsReducerFactory;
import ar.edu.itba.pod.Query2.ThousandMovementsMapper;
import ar.edu.itba.pod.Query2.ThousandMovementsReducerFactory;
import ar.edu.itba.pod.client.Queries.Query;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Query2 implements Query {
    private List<Movement> movements;
    private HazelcastInstance hz;

    public Query2(List<Movement> movements, HazelcastInstance hz) {
        this.movements = movements;
        this.hz = hz;
    }

    @Override
    public void run() throws InterruptedException, ExecutionException {
        /* Add movements list to hazelcast */
        IList<Movement> hzMovement = hz.getList("movements");
        hzMovement.addAll(movements);

        /* Create Query 2 Job */
        JobTracker jobTracker = hz.getJobTracker("Query2");

        /* Oaci movements map, Key is oaci, value is amount of movements */
        Map<String, Integer> oaciMovementsMap = getOaciMovmentsMap(jobTracker, hzMovement);

        /* Remove Movements List from Hazelcast */
        hzMovement.destroy();

        /* Get movements group amount
         * Key is thousand of movements, Value is a list of oaci codes */
        Map<Integer, List<String>> thousandOfMovements = getThousandOfMovements(jobTracker, oaciMovementsMap);

        /* Get query output */
        List<QueryOutputRow> queryOutput = getQueryOutput(thousandOfMovements);

        /* Print query output */
        System.out.println(thousandOfMovements);
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
        KeyValueSource<String, Integer> soruce = KeyValueSource.fromMap(hzOaciMovemntsMap);
        Job<String, Integer> job = jobTracker.newJob(soruce);

        /**/
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

        return queryOutput;
    }

    private class QueryOutputRow {
        private final int thousandMovements;
        private final String oaci1;
        private final String oaci2;

        public QueryOutputRow(int thousandMovements, String oaci1, String oaci2) {
            this.thousandMovements = thousandMovements;
            this.oaci1 = oaci1;
            this.oaci2 = oaci2;
        }
    }
}
