package ZooKeeper;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * @author niyaolanggeyo
 * @date 2021/7/9 15:16
 */
public class DistributeServer {
    private static final int SESSION_TIMEOUT = 2000;
    private static final String CONNECT_STRING = "hadoop101:2181,hadoop102:2181,hadoop103:2181,hadoop104:2181,hadoop105:2181";
    private ZooKeeper zk;

    public static void main(String[] args) throws InterruptedException, KeeperException, IOException {
        DistributeServer server = new DistributeServer();

        server.getConnect();
        server.register(args[0]);
        server.business();


    }

    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    private void register(String arg) throws InterruptedException, KeeperException {
        String create = zk.create("/servers" + arg, arg.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    private void getConnect() throws IOException {
        zk = new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {

            }
        });
    }
}
