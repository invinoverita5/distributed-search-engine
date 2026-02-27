package cis5550.flame;

import java.util.*;

import cis5550.flame.FlamePairRDD.*;
import cis5550.kvs.*;
import cis5550.tools.*;

class FlameRDDImpl implements FlameRDD {
    final FlameContextImpl context;
    String tableName;
    boolean destroyed = false;

    public FlameRDDImpl(FlameContextImpl context, String tableName) {
        this.context = context;
        this.tableName = tableName;
    }

    @Override
    public int count() throws Exception {
        checkDestroyed();
        return context.getKVS().count(tableName);
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
            kvs.delete(tableNameArg);
            kvs.putRowBatch(tableNameArg, batch, false);
            kvs.delete(tableName);
        }

        tableName = tableNameArg;
    }

    @Override
    public FlameRDD distinct() throws Exception {
        checkDestroyed();
        String outputTableName = "rdd-" + UUID.randomUUID();
        context.invokeDistinctOperation(tableName, outputTableName);
        return new FlameRDDImpl(context, outputTableName);
    }

    @Override
    public void destroy() throws Exception {
        checkDestroyed();
        context.getKVS().delete(tableName);
        destroyed = true;
    }

    @Override
    public Vector<String> take(int num) throws Exception {
        checkDestroyed();
        Vector<String> result = new Vector<>();
        Iterator<Row> rows = context.getKVS().scan(tableName);

        int count = 0;
        while (rows.hasNext() && count < num) {
            Row row = rows.next();
            String value = row.get("value");
            if (value != null) {
                result.add(value);
                ++count;
            }
        }

        return result;
    }

    @Override
    public String fold(String zeroElement, TwoStringsToString lambda) throws Exception {
        checkDestroyed();
        String intermediateTableName = "rdd-" + UUID.randomUUID();
        context.invokeOperation("/rdd/fold", tableName, intermediateTableName, Serializer.objectToByteArray(lambda),
                zeroElement);

        KVSClient kvs = context.getKVS();
        Iterator<Row> rows = kvs.scan(intermediateTableName);
        String accumulator = zeroElement;

        while (rows.hasNext()) {
            Row row = rows.next();
            String value = row.get("value");
            if (value != null) {
                accumulator = lambda.op(accumulator, value);
            }
        }

        kvs.delete(intermediateTableName);
        return accumulator;
    }

    @Override
    public List<String> collect() throws Exception {
        checkDestroyed();
        List<String> result = new ArrayList<>();
        KVSClient kvs = context.getKVS();
        Iterator<Row> rows = kvs.scan(tableName);
        while (rows.hasNext()) {
            Row row = rows.next();
            String value = row.get("value");
            if (value != null) {
                result.add(value);
            }
        }
        return result;
    }

    @Override
    public FlameRDD flatMap(StringToIterable lambda) throws Exception {
        checkDestroyed();
        String outputTableName = "rdd-" + UUID.randomUUID();
        context.invokeOperation("/rdd/flatMap", tableName, outputTableName, Serializer.objectToByteArray(lambda));
        return new FlameRDDImpl(context, outputTableName);
    }

    @Override
    public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
        checkDestroyed();
        String outputTableName = "pairrdd-" + UUID.randomUUID();
        context.invokeOperation("/rdd/flatMapToPair", tableName, outputTableName, Serializer.objectToByteArray(lambda));
        return new FlamePairRDDImpl(context, outputTableName);
    }

    @Override
    public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
        checkDestroyed();
        String outputTableName = "pairrdd-" + UUID.randomUUID();
        context.invokeOperation("/rdd/mapToPair", tableName, outputTableName, Serializer.objectToByteArray(lambda));
        return new FlamePairRDDImpl(context, outputTableName);
    }

    @Override
    public FlameRDD subtract(String other) throws Exception {
        checkDestroyed();
        String outputTableName = "rdd-" + UUID.randomUUID();
        context.invokeOperation("/rdd/subtract", tableName, other, outputTableName, null, null);
        return new FlameRDDImpl(context, outputTableName);
    }

    @Override
    public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
        checkDestroyed();
        String outputTableName = "rdd-" + UUID.randomUUID();
        context.invokeOperation("/rdd/mapPartitions", tableName, outputTableName, Serializer.objectToByteArray(lambda));
        return new FlameRDDImpl(context, outputTableName);
    }

    private void checkDestroyed() {
        if (destroyed) {
            throw new IllegalStateException("RDD has been destroyed");
        }
    }

    public String getTableName() {
        return tableName;
    }

}
