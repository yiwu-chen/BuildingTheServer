import java.io.BufferedReader;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

/**
 * Producer will read lines from the text file and put the lines into the blockingqueue.
 * At the end, it will put the end marker to the blockingqueue.
 */
class Producer implements Runnable {
  private final BlockingQueue queue;
  private BufferedReader bufferedReader;
  private Logger logger;

  Producer(BlockingQueue q, BufferedReader br, Logger logger) {
    queue = q;
    bufferedReader = br;
  }

  @Override
  public void run() {
    try {
      while (true) {
        String line;
        while ((line = bufferedReader.readLine()) != null) {
          if (!line.isEmpty()) {
            queue.put(line);
          }
        }
        queue.put("EndMarker");
        break;
      }
    } catch (InterruptedException | IOException e) {
      e.printStackTrace();
      logger.info("Error: " + e.getMessage());
    }
  }
}