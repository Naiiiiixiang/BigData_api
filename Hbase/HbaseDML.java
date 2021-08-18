package Hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author niyaolanggeyo
 * @date 2021/7/7 9:49
 */
public class HbaseDML {
    static Connection conn = null;

    static {
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "hadoop101,hadoop102,hadoop103.hadoop104,hadoop105");
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            System.out.println("Get connection failed.");
        }
    }

    public static void closeConnection() throws IOException {
        conn.close();
    }

    /**
     * @param namespace
     * @param tableName
     * @param rowKey
     * @param family
     * @param column
     * @param value
     * @throws IOException
     */
    public static void putCell(String namespace, String tableName, String rowKey, String family, String column, String value) throws IOException {
        Table table = conn.getTable(TableName.valueOf(namespace, tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

    /**
     * @param namespace
     * @param tableName
     * @param rowKey
     * @param family
     * @param column
     * @throws IOException
     */
    public static void getCell(String namespace, String tableName, String rowKey, String family, String column) throws IOException {
        Table table = conn.getTable(TableName.valueOf(namespace, tableName));

        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
        get.readAllVersions();
        get.readVersions(222);

        Result result = table.get(get);

        System.out.println(new String(result.value()));
        System.out.println("==================================");

        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println(CellUtil.cloneValue(cell));
        }

        CellScanner cellScanner = result.cellScanner();
        while (cellScanner.advance()) {
            Cell current = cellScanner.current();
            System.out.println(new String(CellUtil.cloneValue(current)));
        }

        table.close();
    }

    /**
     * @param namespace
     * @param tableName
     * @param startRow
     * @param stopRow
     * @throws IOException
     */
    public static void scanTable(String namespace, String tableName, String startRow, String stopRow) throws IOException {
        Table table = conn.getTable(TableName.valueOf(namespace, tableName));

        Scan scan = new Scan()
                .withStartRow(Bytes.toBytes(startRow))
                .withStopRow(Bytes.toBytes(stopRow));

        System.out.println("Does I in raw mode" + scan.isRaw());
        scan.setRaw(true);
        System.out.println("Does I in raw mode" + scan.isRaw());

        scan.readVersions(222);

        ResultScanner resultScanner = table.getScanner(scan);

        for (Result result : resultScanner) {
            System.out.println("I am a result.");
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println(new String(CellUtil.cloneValue(cell)));
            }
        }

        table.close();

    }

    /**
     * @param namespace
     * @param tableName
     * @param rowKey
     * @param family
     * @param column
     * @throws IOException
     */
    public static void deleteCells(String namespace, String tableName, String rowKey, String family, String column) throws IOException {
        Table table = conn.getTable(TableName.valueOf(namespace, tableName));

        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));

        table.delete(delete);
        table.close();
    }

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        for (int i = 18; i <= 30; i++) {
            putCell("book", "BigData", "1001", "info", "age", Integer.toString(i));
        }

        getCell("book", "BigData", "1001", "info", "age");

        scanTable("book", "BigData", "1001", "1010");

        deleteCells("book", "BigData", "1001", "info", "age");

        System.out.println(Bytes.toHex(Bytes.toBytes("jhgdsfyvfjewsj")));

        conn.close();
    }

}
