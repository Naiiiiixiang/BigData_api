package Hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @author niyaolanggeyo
 * @date 2021/7/7 9:48
 */
public class HbaseConnect {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorm", "hadoop101,hadoop102,hadoop103,hadoop104,hadoop105");

        Connection connection = ConnectionFactory.createConnection(conf);
        System.out.println(connection);
        connection.close();
    }
}
