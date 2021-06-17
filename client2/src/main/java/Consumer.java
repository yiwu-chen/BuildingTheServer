import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import io.swagger.client.ApiException;
import io.swagger.client.ApiResponse;
import io.swagger.client.api.TextbodyApi;
import io.swagger.client.model.ResultVal;
import io.swagger.client.model.TextLine;

/**
 * Consumer will take the lines from the blockingqueue, and sends the line from the file to the server.
 * If the end marker is taken by a consumer, it will put the end marker to the blockingqueue for the next consumer to use.
 */
class Consumer implements Runnable {
  private final BlockingQueue<String> queue;
  private TextbodyApi api;
  private CyclicBarrier barrier;
  private Client counter;
  public int successfullCount = 0;
  public int unsuccessfullCount = 0;
  private AtomicInteger successfulRequests;
  private AtomicInteger unsuccessfulRequests;
  private ArrayList<ResponseRecord> responseRecords;
  private Logger logger;

  Consumer(BlockingQueue q, TextbodyApi api, CyclicBarrier barrier,
           Client counter, AtomicInteger successfulRequests, AtomicInteger unsuccessfulRequests,
           ArrayList<ResponseRecord> responseRecords, Logger logger) {
    queue = q;
    this.api = api;
    this.barrier = barrier;
    this.counter = counter;
    this.successfulRequests = successfulRequests;
    this.unsuccessfulRequests = unsuccessfulRequests;
    this.responseRecords = responseRecords;
    this.logger = logger;
  }

  @Override
  public void run() {
    try {
      counter.inc();
      long startTime = 0;
      long endTime = 0;
      long latency = 0;
      while (true) {
        String line = queue.take();
        if (line.equals("EndMarker")) {
          queue.put("EndMarker");
          break;
        }
        TextLine body = new TextLine();
        body.setMessage(line);
        String function = "wordcount";

        startTime = System.currentTimeMillis();
        //Posts a new line of text for analysis
        ApiResponse<ResultVal> res = api.analyzeNewLineWithHttpInfo(body, function);
        //System.out.println(res.getData().getMessage().toString());

        endTime = System.currentTimeMillis();

        latency = endTime - startTime;
        int responseCode = res.getStatusCode();
        responseRecords.add(new ResponseRecord(startTime, "POST", latency, responseCode));
        successfullCount += 1;
      }
      //System.out.println("Thread waiting at barrier");
    } catch (InterruptedException | ApiException e) {
      e.printStackTrace();
      unsuccessfullCount += 1;
      logger.info("Error: " + e.getMessage());
    }
    try {
      barrier.await();
    } catch (BrokenBarrierException e) {
      e.printStackTrace();
      logger.info("Error: " + e.getMessage());
    } catch (InterruptedException e) {
      e.printStackTrace();
      logger.info("Error: " + e.getMessage());
    }
    successfulRequests.addAndGet(successfullCount);
    unsuccessfulRequests.addAndGet(unsuccessfullCount);
  }
}