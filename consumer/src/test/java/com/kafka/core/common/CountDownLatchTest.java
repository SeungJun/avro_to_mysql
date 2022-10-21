package com.kafka.core.common;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class CountDownLatchTest {


	private final static int THREADS = 10;
	private static CountDownLatch latch = new CountDownLatch(THREADS);

	public static class RandomSleepRunnable implements Runnable {
		private int id = 0;

		private static Random random = new Random(System.currentTimeMillis());

		public RandomSleepRunnable(int id) {
			this.id = id;

		}

		@Override
		public void run() {
			System.out.println(Thread.currentThread().getName() + " : Thread(" + id + ") : Start.");
			// 1000ms 에서 2000ms 사이의 딜레이 값을 랜덤하게 생성.
			int delay = random.nextInt(1001) + 3000;
			try {
				System.out.println("Thread(" + id + ") : Sleep " + delay + "ms");
				Thread.sleep(delay);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println("Thread(" + id + ") : End.");
			//latch 의 카운터에서 - 1.
			latch.countDown();
		}
	}

	public static void main(String[] args) {

		for (int i = 0; i < THREADS; i++) {
			new Thread(new RandomSleepRunnable(i)).start();
		}

		try {
			// lacth 의 카운트가 0이 될 때 까지 대기한다.
			latch.await();
			// 아래와 같이 TimeOut 을 설정할 수 있다.
			// 아래의 경우는, 만약 2000ms 동안 latch 의 카운트가 0 되지 않는다면
			// wait 상태를 해제하고 다음 동작으로 넘어간다.
//             latch.await(2000, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		System.out.println("All threads are terminated");

	}
}
