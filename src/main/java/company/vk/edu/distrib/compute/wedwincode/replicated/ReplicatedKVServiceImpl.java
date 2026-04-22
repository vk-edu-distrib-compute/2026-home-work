package company.vk.edu.distrib.compute.wedwincode.replicated;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.ReplicatedService;
import company.vk.edu.distrib.compute.wedwincode.DaoRecord;
import company.vk.edu.distrib.compute.wedwincode.KVServiceImpl;
import company.vk.edu.distrib.compute.wedwincode.exceptions.QuorumException;
import company.vk.edu.distrib.compute.wedwincode.exceptions.ServiceStopException;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletionService;
import java.util.function.Function;

public class ReplicatedKVServiceImpl extends KVServiceImpl implements ReplicatedService {
    private static final int DEFAULT_ACK = 1;
    private static final String STATS_PREFIX = "/v0/stats/replica/";

    private final int portNumber;
    private final List<Dao<DaoRecord>> replicas;
    private final boolean[] enabled;

    private final ReplicationService replicationService;
    private final ReplicationStatsService statsService;

    public ReplicatedKVServiceImpl(int port, List<Dao<DaoRecord>> replicas) throws IOException {
        super(port, null);
        this.portNumber = port;
        this.replicas = replicas;
        enabled = new boolean[replicas.size()];
        Arrays.fill(enabled, true);
        replicationService = new ReplicationService(replicas, i -> enabled[i]);
        statsService = new ReplicationStatsService(replicas.size(), STATS_PREFIX);
        this.server.createContext(STATS_PREFIX, statsService::handleStats);
    }

    @Override
    public int port() {
        return portNumber;
    }

    @Override
    public int numberOfReplicas() {
        return replicas.size();
    }

    @Override
    public void disableReplica(int nodeId) {
        if (nodeId > replicas.size()) {
            throw new IllegalArgumentException("incorrect nodeId");
        }
        enabled[nodeId] = false;
    }

    @Override
    public void enableReplica(int nodeId) {
        if (nodeId > replicas.size()) {
            throw new IllegalArgumentException("incorrect nodeId");
        }
        enabled[nodeId] = true;
    }

    @Override
    protected void closeDao() {
        boolean success = true;
        for (Dao<DaoRecord> replica: replicas) {
            try {
                replica.close();
            } catch (IOException e) {
                success = false;
            }
        }
        if (!success) {
            throw new ServiceStopException("one or multiple dao was not closed successfully");
        }
    }

    @Override
    protected void handleEntityMethod(Map<String, String> params, HttpExchange exchange) throws IOException {
        int ack = getAck(params);
        if (ack > replicas.size()) {
            sendEmptyResponse(HttpURLConnection.HTTP_BAD_REQUEST, exchange);
            return;
        }
        super.handleEntityMethod(params, exchange);
    }

    @Override
    protected DaoRecord getValue(Map<String, String> params) {
        String id = getValueFromParams("id", params);
        int ack = getAck(params);

        Function<Integer, ReplicationService.ReadResult> taskGet = replicaId -> {
            statsService.incrementRequestCount(replicaId);
            try {
                return new ReplicationService.ReadResult(true, replicas.get(replicaId).get(id));
            } catch (NoSuchElementException e) {
                return new ReplicationService.ReadResult(true, null);
            } catch (IOException e) {
                return new ReplicationService.ReadResult(false, null);
            }
        };

        CompletionService<ReplicationService.ReadResult> cs = replicationService.createTasks(taskGet);
        List<ReplicationService.ReadResult> results = replicationService.getResults(
                cs,
                ack,
                ReplicationService.ReadResult::responded
        );
        ReplicationService.AggregationResult aggregationResult = replicationService.aggregateResults(results);

        if (aggregationResult.responded() < ack) {
            throw new QuorumException("id not found");
        }
        if (aggregationResult.best() == null || aggregationResult.best().deleted()) {
            throw new NoSuchElementException("id was deleted");
        }

        return aggregationResult.best();
    }

    @Override
    protected void putValue(Map<String, String> params, byte[] data) {
        String id = getValueFromParams("id", params);
        int ack = getAck(params);

        DaoRecord daoRecord = DaoRecord.buildCreated(data);

        Function<Integer, Boolean> taskUpsert = replicaId -> {
            statsService.incrementRequestCount(replicaId);
            try {
                replicas.get(replicaId).upsert(id, daoRecord);
                statsService.incrementKeysCount(replicaId);
                return true;
            } catch (Exception e) {
                return false;
            }
        };

        CompletionService<Boolean> cs = replicationService.createTasks(taskUpsert);
        List<Boolean> results = replicationService.getResults(cs, ack, Boolean.TRUE::equals);

        int confirmed = (int) results.stream().filter(Boolean.TRUE::equals).count();
        if (confirmed < ack) {
            throw new QuorumException("error upserting data");
        }
    }

    @Override
    protected void deleteValue(Map<String, String> params) {
        String id = getValueFromParams("id", params);
        int ack = getAck(params);

        Function<Integer, Boolean> taskDelete = replicaId -> {
            statsService.incrementRequestCount(replicaId);
            try {
                replicas.get(replicaId).delete(id);
                return true;
            } catch (Exception e) {
                return false;
            }
        };

        CompletionService<Boolean> cs = replicationService.createTasks(taskDelete);
        List<Boolean> results = replicationService.getResults(cs, ack, Boolean.TRUE::equals);

        int confirmed = (int) results.stream().filter(Boolean.TRUE::equals).count();
        if (confirmed < ack) {
            throw new QuorumException("error deleting data");
        }
    }

    private static int getAck(Map<String, String> params) {
        try {
            String ackRaw = getValueFromParams("ack", params);
            return Integer.parseInt(ackRaw);
        } catch (IllegalArgumentException e) {
            return DEFAULT_ACK;
        }
    }
}
