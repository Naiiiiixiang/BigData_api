package ZooKeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author niyaolanggeyo
 * @date 2021/7/9 15:17
 */
public class DistributeLock {
    private static final int SESSION_TIMEOUT = 2000;
    private static final String CONNECT_STRING = "hadoop101:2181,hadoop102:2181,hadoop103:2181,hadoop104:2181,hadoop105:2181";
    private ZooKeeper zk;

    private final CountDownLatch connectLatch = new CountDownLatch(1);
    private final CountDownLatch waitLatch = new CountDownLatch(1);

    private String waitPath;
    private String currentMode;

    /**
     * @throws IOException
     * @throws InterruptedException
     * @throws KeeperException
     */
    public DistributeLock() throws IOException, InterruptedException, KeeperException {
        zk = new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    connectLatch.countDown();
                }

                if (event.getType() == Event.EventType.NodeDeleted && event.getPath().equals(waitPath)) {
                    waitLatch.countDown();
                }
            }
        });

        connectLatch.await();

        Stat stat = zk.exists("/locks", false);

        if (stat == null) {
            zk.create("/locks", "locks".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    public void zkLock() {
        try {
            currentMode = zk.create("/locks" + "seq-", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

            Thread.sleep(10);

            List<String> children = zk.getChildren("/locks", false);

            if (children.size() == 1) {
                return;
            } else {
                Collections.sort(children);

                String thisNode = currentMode.substring("/locks".length());

                int index = children.indexOf(thisNode);
                if (index == -1) {
                    System.out.println("Data Error");
                } else if (index == 0) {
                    return;
                } else {
                    waitPath = "/locks" + children.get(index - 1);
                    zk.getData(waitPath, true, new Stat());
                    waitLatch.await();
                    return;
                }
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void unZkLock() {
        try {
            zk.delete(this.currentMode, 222);
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }
}
