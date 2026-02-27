package cis5550.kvs;

public class Table {
    public static boolean isPersistent(String tableName) {
        return tableName.startsWith("pt-");
    }
}
