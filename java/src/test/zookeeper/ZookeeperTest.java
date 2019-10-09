package zookeeper;

import com.tzl.util.ZookeeperUtil;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ZookeeperTest {
    public static void main(String[] args) throws Exception {
        final ZookeeperUtil zookeeperUtil = new ZookeeperUtil();
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                zookeeperUtil.start("localhost:2181,localhost:2182,localhost:2183","test");
                countDownLatch.countDown();
                System.out.println("解除阻塞");
            }
        });
        thread.start();
        System.out.println("阻塞开始");
        countDownLatch.await();
        System.out.println("阻塞结束");
        zookeeperUtil.setTemporaryNode("/hello","OK");
        System.out.println(zookeeperUtil.hasNodeExits("/hello"));
        System.out.println(zookeeperUtil.getNodeValue("/hello2"));
        testSynchronizedNode(zookeeperUtil,50,"/node");
    }

    private static void testSynchronizedNode(final ZookeeperUtil zookeeperUtil,final int size,final String nodeName) throws Exception {
        final AtomicInteger atomicInteger = new AtomicInteger();
        final CountDownLatch countDownLatch = new CountDownLatch(size);
        for (int i = 0; i < size; i++) {
            Future<Number> result = zookeeperUtil.synchronizedNode(nodeName, new Callable<Number>() {
                @Override
                public Number call(){
                    int result = atomicInteger.incrementAndGet();
                    System.out.println(nodeName+"---"+result);
                    countDownLatch.countDown();
                    return 0;
                }
            });
        }
        countDownLatch.await();
        System.out.println(atomicInteger.get());
    }

    private static void testSynchronized(final ZookeeperUtil zookeeperUtil,final int size,final String nodeName) throws Exception {
        final AtomicInteger atomicInteger = new AtomicInteger();
        final CountDownLatch countDownLatch = new CountDownLatch(size);
        ExecutorService service = Executors.newFixedThreadPool(8);
        for (int i = 0; i < size; i++) {
            service.submit(new Callable<Number>() {
                @Override
                public Number call(){
                    int result = atomicInteger.incrementAndGet();
                    System.out.println(nodeName+"---"+result);
                    countDownLatch.countDown();
                    return 0;
                }
            });
        }
        countDownLatch.await();
        System.out.println(atomicInteger.get());
    }
}

