import java.io.*;
import java.nio.file.Paths;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;

import io.swagger.client.*;
import io.swagger.client.api.TextbodyApi;

import java.io.File;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;


public class Client {
  //private static String url = "http://localhost:8080/Server_war_exploded/textbody/wordcount";
  private static String url = "http://networklb-7a0859d5c0b2cd47.elb.us-east-1.amazonaws.com:8080/Server_war/textbody/wordcount";
  private static int numThreads;
  private static String filePath;
  static CyclicBarrier barrier;
  static int count = 0;
  static AtomicInteger successfulRequests = new AtomicInteger();
  static AtomicInteger unsuccessfulRequests = new AtomicInteger();
  private static ArrayList<ResponseRecord> responseRecords;

  private static List<Integer> latencyRecords;
  private static int size;
  private static double mean;
  private static double medium;
  private static double totalWallTime;
  private static double throughput;
  private static double nintyninthPercentile;
  private static double maxResponseTime;
  private static Logger logger;

  public synchronized void inc() {
    count++;
  }

  public int getVal() {
    return this.count;
  }


  public static void main(String[] args) throws InterruptedException,
          IOException, BrokenBarrierException {
    //args[0]: an input file. Use this one for testing
    //args[1]: number of threads to run - maxThreads
    filePath = args[0];
    numThreads = Integer.parseInt(args[1]);

    //Process and validate the input parameters
    File file = new File(filePath);
    BufferedReader br = new BufferedReader(new FileReader(file));
    BlockingQueue<String> queue = new LinkedBlockingQueue<>();
    barrier = new CyclicBarrier(numThreads + 1);
    final Client counter = new Client();
    responseRecords = new ArrayList<>();
    latencyRecords = new ArrayList<>();
    //One producer to read lines from the file.
    Producer producer = new Producer(queue, br, logger);
    new Thread(producer).start();

    ApiClient apiClient = new ApiClient().setBasePath(url);
    final TextbodyApi api = new TextbodyApi(apiClient);
    long startTime = System.currentTimeMillis();

    //Multiple consumers that continuously send a line from the file to the server.
    Consumer[] consumers = new Consumer[numThreads];
    for (int i = 0; i < numThreads; i++) {
      consumers[i] = new Consumer(queue, api, barrier, counter, successfulRequests,
              unsuccessfulRequests, responseRecords, logger);
      new Thread(consumers[i]).start();
    }
    barrier.await();
    //Write the records to the CSV file
    //writeCSVFile("ReponseRecordsP2.csv", "", responseRecords);

    long endTime = System.currentTimeMillis();
    double wallTime = (double) (endTime - startTime) / 1000;
    System.out.println("Total number of threads: " + numThreads);
    System.out.println("Total number of successful requests: " + successfulRequests);
    System.out.println("Total number of unsuccessful requests: " + unsuccessfulRequests);
    System.out.println("The total run time(wall time): " + wallTime + " seconds");
    int totalRequests = successfulRequests.intValue() + unsuccessfulRequests.intValue();
    System.out.println("Throughput: " + totalRequests / wallTime + " requests/seconds");
    System.out.println("Total running threads should be equal to " + numThreads + ". It is: "
            + counter.getVal());

    createLatencyRecordsList(responseRecords);

    size = responseRecords.size();
    mean = calculateMean(latencyRecords);
    medium = calculateMedium(latencyRecords);
    totalWallTime = calculateWallTime(responseRecords);
    throughput = successfulRequests.intValue() / (totalWallTime / 1000);
    nintyninthPercentile = calculateNintyninthPercentile(latencyRecords);
    maxResponseTime = calculateMaxResponseTime(latencyRecords);

    System.out.println("Latency process result:");
    System.out.println("Total number of threads: " + numThreads);
    System.out.println("Mean response time: " + mean + " milliseconds");
    System.out.println("Median response time: " + medium + " milliseconds");
    System.out.println("The total wall time: " + totalWallTime + " milliseconds");
    System.out.println("Throughput: " + throughput + " requests/second");
    System.out.println("P99 response time: " + nintyninthPercentile + " milliseconds");
    System.out.println("Max response time: " + maxResponseTime + " milliseconds");
  }

  public static void writeCSVFile(String filename, String filepath,
                                  ArrayList<ResponseRecord> responses) throws IOException {
    FileWriter fileWriter = new FileWriter(String.valueOf(Paths.get(filepath, filename)));
    fileWriter.write("\"Start Time\",\t\"Request Type\",\t\"Latency\",\t\"Response Code\"\n");
    for (ResponseRecord response : responses) {
      fileWriter.write(response.toString());
    }
    fileWriter.close();
  }

  private static void createLatencyRecordsList(ArrayList<ResponseRecord> responseRecords) {
    for (ResponseRecord response : responseRecords) {
      latencyRecords.add((int) response.getLatency());
    }
  }

  private static double calculateMean(List<Integer> responseTimes) {
    double sum = 0.0;
    for (int responseTime : responseTimes) {
      sum += responseTime;
    }
    return sum / size;
  }

  private static double calculateMedium(List<Integer> responseTimes) {
    Collections.sort(responseTimes);
    double median;
    if (size % 2 == 0)
      return median = ((double) responseTimes.get(size / 2)
              + (double) responseTimes.get(size / 2 - 1) / 2);
    else
      return median = (double) responseTimes.get(size / 2);
  }

  private static double calculateWallTime(ArrayList<ResponseRecord> responseRecords) {
    return responseRecords.get(size - 1).getStartTime() - responseRecords.get(0).getStartTime()
            + responseRecords.get(size - 1).getLatency();
  }

  private static double calculateNintyninthPercentile(List<Integer> responseTimes) {
    return responseTimes.get((int) Math.ceil(size * 0.99) - 1);
  }

  private static double calculateMaxResponseTime(List<Integer> responseTimes) {
    int max = Integer.MIN_VALUE;
    for (Integer response : responseTimes) {
      if (response >= max) {
        max = response;
      }
    }
    return max;
  }
}