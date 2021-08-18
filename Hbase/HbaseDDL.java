package Hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author niyaolanggeyo
 * @date 2021/7/7 9:48
 */
public class HbaseDDL {
    static Connection conn = null;

    static {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorm", "hadoop101,hadoop102,hadoop103,hadoop104,hadoop105");
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            System.out.println("Get connection failed!");
        }
    }

    public static void closeConnection() throws IOException {
        conn.close();
    }

    /**
     * @param nameSpace
     * @return
     * @throws IOException
     */
    public static boolean createNameSpace(String nameSpace) throws IOException {
        Admin admin = conn.getAdmin();

        try {
            NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(nameSpace);
            admin.createNamespace(builder.build());
            admin.close();
            return true;
        } catch (NamespaceExistException namespaceExistException) {
            System.out.println(nameSpace + ": this namesapce has existed.");
            admin.close();
            return false;
        }
    }

    /**
     * @param nameSpace
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean tableExists(String nameSpace, String tableName) throws IOException {
        Admin admin = conn.getAdmin();
        boolean b = admin.tableExists(TableName.valueOf(nameSpace, tableName));
        admin.close();
        return b;
    }

    /**
     * @param nameSpace
     * @param tableName
     * @param families
     * @throws IOException
     */
    public static void createTable(String nameSpace, String tableName, String... families) throws IOException {
        if (tableExists(nameSpace, tableName)) {
            System.out.println(nameSpace + ":" + tableName + " has existed.");
            return;
        }

        if (families.length < 1) {
            System.out.println("Needs to have at least one column family");
            return;
        }

        Admin admin = conn.getAdmin();
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(nameSpace, tableName));
        for (String family : families) {
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family));
            columnFamilyDescriptorBuilder.setMaxVersions(222);
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
        }
        TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
        admin.createTable(tableDescriptor);
        admin.close();
    }

    /**
     * @param nameSpace
     * @param tableName
     * @param family
     * @param version
     * @throws IOException
     */
    public static void alterTable(String nameSpace, String tableName, String family, int version) throws IOException {
        Admin admin = conn.getAdmin();

        TableDescriptor tableDescriptor = admin.getDescriptor(TableName.valueOf(nameSpace, tableName));
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableDescriptor);
        ColumnFamilyDescriptor columnFamily = tableDescriptor.getColumnFamily(Bytes.toBytes(family));
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(columnFamily);
        columnFamilyDescriptorBuilder.setMaxVersions(22222);
        tableDescriptorBuilder.modifyColumnFamily(columnFamilyDescriptorBuilder.build());
        admin.modifyTable(tableDescriptorBuilder.build());
        admin.close();
    }

    /**
     * @param nameSpace
     * @param tableName
     * @throws IOException
     */
    public static void dropTable(String nameSpace, String tableName) throws IOException {
        if (!tableExists(nameSpace, tableName)) {
            System.out.println(nameSpace + ":" + tableName + " is not exists.");
            return;
        }
        Admin admin = conn.getAdmin();
        admin.disableTable(TableName.valueOf(nameSpace, tableName));
        admin.deleteTable(TableName.valueOf(nameSpace, tableName));
        admin.close();
    }

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        System.out.println(conn);
        System.out.println(createNameSpace("book"));
        System.out.println("==================================");
        System.out.println(tableExists("BigData", "lalala001"));
        createTable("BigData", "lalala002", "info", "msg", "dedede");
        alterTable("BigData", "lalala002", "msg", 666);
        dropTable("BigData", "lalala");
        closeConnection();
    }
}
