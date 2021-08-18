package ZooKeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author niyaolanggeyo
 * @date 2021/6/29 13:46
 */
public class zooKeeper {
    private ZooKeeper zkClient;

    @Before
    /**
     * initialize ZooKeeper client
     */
    public void init() throws IOException {
        String connectString = "hadoop101:2181;hadoop102:2181;hadoop103:2181;hadoop104:2181;hadoop105:2181;";
        int sessionTimeout = 10000;
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
            }
        });
    }

    @After
    /**
     * close client object
     */
    public void close() throws InterruptedException {
        zkClient.close();
    }

    @Test
    public void subNodeList() throws InterruptedException, KeeperException {
        List<String> zkClientChildren = zkClient.getChildren("/", false);
        System.out.println(zkClientChildren);
    }

    /**
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void subNodeListAndWatched() throws InterruptedException, KeeperException {
        List<String> zkClientChildrenAndWatched = zkClient.getChildren("/hahaha", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event);
            }
        });
        System.out.println(zkClientChildrenAndWatched);

        //because set the watcher, current thread isn't shut down
        Thread.sleep(Long.MAX_VALUE);
    }


    @Test
    public void createSubNode() throws InterruptedException, KeeperException {
        String path = zkClient.create("/hahaha", "lalala".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(path);
    }

    @Test
    public void exist() throws InterruptedException, KeeperException {
        Stat exists = zkClient.exists("/hahaha", false);
        System.out.println(exists == null ? "Not exist." : "Existed.");
    }


    @Test
    public void get() throws InterruptedException, KeeperException {
        Stat stat = zkClient.exists("/hahaha", false);

        if (stat == null) {
            System.out.println("Node is not exist...");
            return;
        }

        byte[] zkClientData = zkClient.getData("/hahaha", false, stat);
        System.out.println(new String(zkClientData));
    }

    @Test
    public void getAndWatched() throws InterruptedException, KeeperException {
        Stat stat = zkClient.exists("/hahaha", false);
        if (stat == null) {
            System.out.println("Node is not exist...");
            return;
        }

        byte[] zkClientData = zkClient.getData("/hahaha", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event);
            }
        }, stat);

        System.out.println(new String(zkClientData));

        //because set the watcher, current thread isn't shut down
        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    public void set() throws InterruptedException, KeeperException {
        Stat stat = zkClient.exists("/hahaha", false);

        if (stat == null) {
            System.out.println("Node is not exist...");
            return;
        }

        zkClient.setData("/hahaha", "lalala".getBytes(), stat.getVersion());
    }


    @Test
    public void delete() throws InterruptedException, KeeperException {
        Stat stat = zkClient.exists("/hahaha", false);

        if (stat == null) {
            System.out.println("Node is not exist...");
            return;
        }

        zkClient.delete("/hahaha", stat.getVersion());
    }


    @Test
    public void deleteAll(String path, ZooKeeper zk) throws InterruptedException, KeeperException {
        Stat stat = zkClient.exists("/hahaha", false);

        if (stat == null) {
            System.out.println("Node is not exist...");
            return;
        }

        List<String> zkClientChildren = zkClient.getChildren(path, false);

        if (!zkClientChildren.isEmpty()) {
            for (String childNode : zkClientChildren) {
                deleteAll(path + "/" + childNode, zk);
            }
        }
        zk.delete(path, stat.getVersion());
    }
}
