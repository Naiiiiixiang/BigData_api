package ZooKeeper;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 * @author niyaolanggeyo
 * @date 2021/7/9 15:17
 */
public class DistributeLockTest {
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        final DistributeLock lock1 = new DistributeLock();
        final DistributeLock lock2 = new DistributeLock();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    lock1.zkLock();
                    System.out.println("Thread_01 started, get lock");
                    Thread.sleep(5000);

                    lock1.unZkLock();
                    System.out.println("Thread_01 released lock");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    lock2.zkLock();
                    System.out.println("Thread_02 started, get lock");
                    Thread.sleep(5000);

                    lock2.unZkLock();
                    System.out.println("Thread_02 released lock");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }
}
