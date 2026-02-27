package cis5550.flame;

import java.net.*;
import java.util.*;

import cis5550.kvs.*;
import cis5550.tools.*;
import cis5550.tools.HTTP.*;
import cis5550.tools.Partitioner.*;
import static cis5550.tools.KeyGenerator.*;

class FlameContextImpl implements FlameContext {
    StringBuilder output = new StringBuilder();
    String jarName;

    public FlameContextImpl(String jarName) {
        this.jarName = jarName;
    }

    @Override
    public KVSClient getKVS() {
        return Coordinator.kvs;
    }

    @Override
    public void output(String s) {
        output.append(s);
    }

    public String getOutput() {
        return output.toString();
    }

    @Override
    public FlameRDD parallelize(List<String> list) throws Exception {
        String tableName = "rdd-" + UUID.randomUUID();

        KVSClient kvs = getKVS();
        for (String value : list) {
            kvs.put(tableName, generateUniqueKey(), "value", value);
        }

        return new FlameRDDImpl(this, tableName);
    }

    @Override
    public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception {
        String outputTableName = "rdd-" + UUID.randomUUID();
        invokeOperation("/fromTable", tableName, outputTableName, Serializer.objectToByteArray(lambda));
        return new FlameRDDImpl(this, outputTableName);
    }

    public void invokeOperation(String operation, String inputTableName, String otherInputTableName,
            String outputTableName, byte[] lambda,
            String zeroElement)
            throws Exception {
        KVSClient kvs = getKVS();
        int numKVSWorkers = kvs.numWorkers();
        if (numKVSWorkers == 0) {
            throw new Exception("No KVS workers available");
        }

        List<String> flameWorkers = Coordinator.getWorkers();
        if (flameWorkers.size() == 0) {
            throw new Exception("No Flame workers available");
        }

        Partitioner partitioner = new Partitioner();

        // register the KVS workers with the partitioner
        TreeMap<String, String> kvsWorkers = new TreeMap<>();
        for (int i = 0; i < numKVSWorkers; ++i) {
            kvsWorkers.put(kvs.getWorkerID(i), kvs.getWorkerAddress(i));
        }

        List<String> kvsWorkerIds = new ArrayList<>(kvsWorkers.keySet());
        for (int i = 0; i < kvsWorkerIds.size(); ++i) {
            String id = kvsWorkerIds.get(i);
            String address = kvsWorkers.get(id);

            // wraparound: last KVS worker is responsible for keys < first KVS worker id
            if (i == numKVSWorkers - 1) {
                partitioner.addKVSWorker(address, id, null);
                partitioner.addKVSWorker(address, null, kvsWorkerIds.get(0));
            } else {
                partitioner.addKVSWorker(address, id, kvsWorkerIds.get(i + 1));
            }
        }

        // register the Flame workers with the partitioner
        for (String flameWorker : flameWorkers) {
            partitioner.addFlameWorker(flameWorker);
        }

        // dispatch HTTP requests to each partition's worker concurrently
        Vector<Partition> partitions = partitioner.assignPartitions();
        int numPartitions = partitions.size();

        Thread[] threads = new Thread[numPartitions];
        Response[] responses = new Response[numPartitions];
        Exception[] exceptions = new Exception[numPartitions];

        for (int i = 0; i < numPartitions; ++i) {
            Partition partition = partitions.get(i);
            final int j = i;

            threads[i] = new Thread(() -> {
                try {
                    String url = "http://" + partition.assignedFlameWorker + operation;
                    url += "?inputTable=" + URLEncoder.encode(inputTableName, "UTF-8");
                    if (otherInputTableName != null) {
                        url += "&otherInputTable=" + URLEncoder.encode(otherInputTableName, "UTF-8");
                    }
                    url += "&outputTable=" + URLEncoder.encode(outputTableName, "UTF-8");
                    url += "&kvsCoordinator=" + URLEncoder.encode(kvs.getCoordinator(), "UTF-8");
                    if (partition.fromKey != null) {
                        url += "&fromKey=" + URLEncoder.encode(partition.fromKey, "UTF-8");
                    }
                    if (partition.toKeyExclusive != null) {
                        url += "&toKeyExclusive=" + URLEncoder.encode(partition.toKeyExclusive, "UTF-8");
                    }
                    if (zeroElement != null) {
                        url += "&zeroElement=" + URLEncoder.encode(zeroElement, "UTF-8");
                    }

                    responses[j] = HTTP.doRequest("POST", url, lambda);
                } catch (Exception e) {
                    exceptions[j] = e;
                }
            });
        }

        for (int i = 0; i < threads.length; ++i) {
            threads[i].start();
        }

        // automatically fail if any threads report failure
        for (int i = 0; i < threads.length; ++i) {
            threads[i].join();
            if (exceptions[i] != null || responses[i] == null) {
                throw new Exception("Partition " + partitions.get(i).toString() + " failed", exceptions[i]);
            } else if (responses[i].statusCode() != 200) {
                throw new Exception(new String(responses[i].body()));
            }
        }
    }

    public void invokeOperation(String operation, String inputTableName, String outputTableName, byte[] lambda)
            throws Exception {
        invokeOperation(operation, inputTableName, null, outputTableName, lambda, null);
    }

    public void invokeOperation(String operation, String inputTableName, String outputTableName, byte[] lambda,
            String zeroElement) throws Exception {
        invokeOperation(operation, inputTableName, null, outputTableName, lambda, null);
    }

    public void invokeDistinctOperation(String inputTableName, String outputTableName)
            throws Exception {
        invokeOperation("/rdd/distinct", inputTableName, null, outputTableName, null, null);
    }

    public void invokeJoinOperation(String inputTableName, String otherInputTableName, String outputTableName)
            throws Exception {
        invokeOperation("/pairrdd/join", inputTableName, otherInputTableName, outputTableName, null, null);
    }

}
