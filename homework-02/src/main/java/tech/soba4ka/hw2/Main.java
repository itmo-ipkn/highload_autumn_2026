package tech.soba4ka.hw2;

import tech.soba4ka.hw2.consumer.ProcessEvents;
import tech.soba4ka.hw2.producer.Generate;
import tech.soba4ka.hw2.verification.Verify;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length == 1 && args[0].equals("verify")) {
            Verify.run();
            return;
        }
        if (args.length != 2) throw new IllegalArgumentException("Expected producer <pod>, consumer <pod> or verify");
        int pod = Integer.parseInt(args[1]);
        var stopping = new AtomicBoolean();
        var finished = new CountDownLatch(1);
        var shutdown = new Thread(() -> {
            stopping.set(true);
            try {
                finished.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "worker-shutdown");
        Runtime.getRuntime().addShutdownHook(shutdown);
        try {
            switch (args[0]) {
                case "producer" -> Generate.run(pod, stopping::get);
                case "consumer" -> ProcessEvents.run(pod, stopping::get);
                default -> throw new IllegalArgumentException(args[0]);
            }
        } finally {
            finished.countDown();
        }
    }
}
