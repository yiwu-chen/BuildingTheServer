import com.google.gson.Gson;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import javax.servlet.http.*;
import javax.servlet.annotation.*;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import io.swagger.client.model.ErrMessage;
import io.swagger.client.model.ResultVal;

/**
 * The servlet class that has doGet and doPost methods to send the response to the client.
 */
@WebServlet(name = "Servlet", value = "/Servlet")
public class Servlet extends HttpServlet {
  private Gson gson = new Gson();
  private Connection conn = null;
  private PooledObjectFactory channelFactory;
  private GenericObjectPool<Channel> channelPool;
  private GenericObjectPoolConfig config;

  private final static String QUEUE_NAME = "hw2Queue";

  public void init() {
    if (conn == null) {
      ConnectionFactory factory = new ConnectionFactory();
      //factory.setHost("localhost");
      factory.setHost("44.193.210.5");
      factory.setPort(5672);
      factory.setUsername("admin");
      factory.setPassword("admin");
      try {
        conn = factory.newConnection();
        channelFactory = new ChannelFactory(conn);
//        config = new GenericObjectPoolConfig();
//        config.setMaxTotal(MAX_POOL_SIZE);
//        channelPool = new GenericObjectPool<>(channelFactory, config);
        channelPool = new GenericObjectPool<>(channelFactory);
        channelPool.setMaxTotal(1000);
        channelPool.setMaxIdle(500);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * The method that send the GET response.
   *
   * @param req the http servlet request
   * @param res the http servlet response
   * @throws IOException when an input or output operation is failed or interpreted
   */
  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
    res.setContentType("text/plain");
    res.setCharacterEncoding("UTF-8");
    String urlPath = req.getPathInfo();
    PrintWriter out = res.getWriter();

    if (urlPath == null || urlPath.isEmpty()) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      out.write("missing parameters");
      return;
    }
    String[] urlParts = urlPath.split("/");
    if (!isUrlValid(urlParts)) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
    } else {
      res.setStatus(HttpServletResponse.SC_OK);
      res.getWriter().write("GET works");
    }
  }

  /**
   * The method that send the POST response.
   *
   * @param req the http servlet request
   * @param res the http servlet response
   * @throws IOException when an input or output operation is failed or interpreted
   */
  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
    res.setContentType("application/json");
    res.setCharacterEncoding("UTF-8");

    String urlPath = req.getPathInfo();
    PrintWriter out = res.getWriter();

    ResultVal resultVal = new ResultVal();
    ErrMessage errMessage = new ErrMessage();

    if (urlPath == null || urlPath.isEmpty()) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      out.write("url is empty or null");
      errMessage.setMessage("Null or empty URL");
      out.write(gson.toJson(errMessage));
      return;
    }
    String[] urlParts = urlPath.split("/");

    if (!isUrlValid(urlParts)) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      errMessage.setMessage("Invalid url");
      out.write(gson.toJson(errMessage));
      return;
    }

    String requestData = req.getReader().lines().collect(Collectors.joining());
    String message = requestData.substring(12, requestData.length() - 2).trim();
    Map<String, Integer> wordsMap = new HashMap<>();
    WordCount.wordCount(wordsMap, message);
    Channel channel = null;
    try {
      channel = channelPool.borrowObject();
      //channel = conn.createChannel();
      channel.queueDeclare(QUEUE_NAME, true, false, false, null);
      for (Map.Entry<String, Integer> entry : wordsMap.entrySet()) {
        StringBuilder tuple = new StringBuilder();
        tuple.append(entry.getKey());
        tuple.append(" , ");
        tuple.append(entry.getValue());
        channel.basicPublish("", QUEUE_NAME, null, tuple.toString().getBytes(StandardCharsets.UTF_8));
      }

    } catch (Exception ex) {
      Logger.getLogger(Servlet.class.getName()).log(Level.SEVERE, null, ex);
    } finally {
      if (channel != null) {
        try {
          channelPool.returnObject(channel);
          //channel.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
    int totalUniqueWords = wordsMap.size();
    resultVal.setMessage(totalUniqueWords);
    out.write(gson.toJson(resultVal));
    res.setStatus(HttpServletResponse.SC_OK);

    out.close();
    out.flush();
    //conn.close();
  }

  /**
   * Validate the url to see if it is valid.
   * Currently, only support the wordCount function, so only textboday/wordcount is valid.
   *
   * @param urlParts all the parts of the url
   * @return result of the validation
   */
  private boolean isUrlValid(String[] urlParts) {
    return urlParts[1].equals("textbody") && urlParts[2].equals("wordcount");
  }
}
