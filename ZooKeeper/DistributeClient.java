package ZooKeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author niyaolanggeyo
 * @date 2021/7/9 15:16
 */
public class DistributeClient {
    private static final int SESSION_TIMEOUT = 2000;
    private static final String CONNECT_STRING = "hadoop101:2181,hadoop102:2181,hadoop103:2181,hadoop104:2181,hadoop105:2181";
    private ZooKeeper zk;

    public static void main(String[] args) throws InterruptedException, KeeperException, IOException {
        DistributeClient client = new DistributeClient();

        client.getConnect();
        client.getServerList();
        client.business();
    }

    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    private void getServerList() throws InterruptedException, KeeperException {
        List<String> children = zk.getChildren("/servers", true);
        ArrayList<String> servers = new ArrayList<String>();

        for (String child : children) {
            byte[] data = zk.getData("/server" + child, false, null);
            servers.add(new String(data));
        }
        System.out.println(servers);
    }

    private void getConnect() throws IOException {
        zk = new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try {
                    getServerList();
                } catch (InterruptedException | KeeperException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
