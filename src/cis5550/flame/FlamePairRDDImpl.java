package cis5550.flame;

import java.util.*;

import cis5550.kvs.*;
import cis5550.tools.*;

class FlamePairRDDImpl implements FlamePairRDD {
    final FlameContextImpl context;
    String tableName;
    boolean destroyed = false;

    public FlamePairRDDImpl(FlameContextImpl context, String tableName) {
        this.context = context;
        this.tableName = tableName;
    }

    @Override
    public List<FlamePair> collect() throws Exception {
        checkDestroyed();
        List<FlamePair> result = new ArrayList<>();
        KVSClient kvs = context.getKVS();
        Iterator<Row> rows = kvs.scan(tableName);
        while (rows.hasNext()) {
            Row row = rows.next();
            Set<String> columns = row.columns();
            for (String column : columns) {
                String value = row.get(column);
                if (value != null) {
                    result.add(new FlamePair(row.key(), row.get(column)));
                }
            }
        }
        return result;
    }

    @Override
    public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
        checkDestroyed();
        String outputTableName = "pairrdd-" + UUID.randomUUID();
        context.invokeOperation("/pairrdd/foldByKey", tableName, outputTableName,
                Serializer.objectToByteArray(lambda));
        return new FlamePairRDDImpl(context, outputTableName);
    }

    @Override
    public void saveAsTable(String tableNameArg) throws Exception {
        checkDestroyed();
        KVSClient kvs = context.getKVS();

        if (Table.isPersistent(tableName) == Table.isPersistent(tableNameArg)) {
            kvs.rename(tableName, tableNameArg);
        } else {
            Iterator<Row> rows = kvs.scan(tableName);
            List<Row> batch = new ArrayList<>();
            while (rows.hasNext()) {
                batch.add(rows.next());
            }
            kvs.putRowBatch(tableNameArg, batch, false);
            kvs.delete(tableName);
        }

        tableName = tableNameArg;
    }

    @Override
    public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
        checkDestroyed();
        String outputTableName = "rdd-" + UUID.randomUUID();
        context.invokeOperation("/pairrdd/flatMap", tableName, outputTableName, Serializer.objectToByteArray(lambda));
        return new FlameRDDImpl(context, outputTableName);
    }

    @Override
    public void destroy() throws Exception {
        checkDestroyed();
        context.getKVS().delete(tableName);
        destroyed = true;
    }

    @Override
    public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
        checkDestroyed();
        String outputTableName = "pairrdd-" + UUID.randomUUID();
        context.invokeOperation("/pairrdd/flatMapToPair", tableName, outputTableName,
                Serializer.objectToByteArray(lambda));
        return new FlamePairRDDImpl(context, outputTableName);
    }

    @Override
    public FlamePairRDD join(FlamePairRDD other) throws Exception {
        checkDestroyed();
        String outputTableName = "pairrdd-" + UUID.randomUUID();
        context.invokeJoinOperation(tableName, other.getTableName(), outputTableName);
        return new FlamePairRDDImpl(context, outputTableName);
    }

    private void checkDestroyed() {
        if (destroyed) {
            throw new IllegalStateException("PairRDD has been destroyed");
        }
    }

    public String getTableName() {
        return tableName;
    }

}
