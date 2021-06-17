import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Consumer class that consume messages sent from the RabbitMQ and create an entry in hashmap
 * for each unique word received.
 */
public class Consumer {
  private final static String QUEUE_NAME = "hw2Queue";
  private static int NUM_THREADS;
  private static Connection conn = null;

  public static void main(String[] args) throws Exception {
    NUM_THREADS = Integer.parseInt(args[0]);
    Map<String, Integer> resultsMap = new ConcurrentHashMap<>();

    if (conn == null) {
      ConnectionFactory factory = new ConnectionFactory();
      //factory.setHost("localhost");
      factory.setHost("44.193.210.5");
      factory.setPort(5672);
      factory.setUsername("admin");
      factory.setPassword("admin");
      conn = factory.newConnection();
    }

    Runnable runnable = new Runnable() {
      @Override
      public void run() {
        try {
          Channel channel = conn.createChannel();
          channel.queueDeclare(QUEUE_NAME, true, false, false, null);
          // max one message per receiver
          channel.basicQos(1);
          //System.out.println(" [*] Thread waiting for messages. To exit press CTRL+C");
          DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            String[] values = message.split(" , ");
            String word = values[0];
            int count = Integer.parseInt(values[1]);
            if (resultsMap.containsKey(word)) {
              Integer value = resultsMap.get(word);
              if (value != null) {
                resultsMap.computeIfPresent(word, (key, oldValue) -> oldValue + count);
              }
            } else {
              resultsMap.put(word, count);
            }
          };
          channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {
          });
          //channel.close();
        } catch (Exception ex) {
          Logger.getLogger(Consumer.class.getName()).log(Level.SEVERE, null, ex);
        }
      }
    };

    //Start threads and block to receive messages
    for (int i = 0; i < NUM_THREADS; i++) {
      Thread recv = new Thread(runnable);
      recv.start();
      recv.join();
    }
  }
}
