import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.io.IOException;
import java.util.concurrent.TimeoutException;


public class ChannelFactory extends BasePooledObjectFactory<Channel> {
  private Connection conn;
  public ChannelFactory(Connection conn){
    this.conn = conn;
  }

  @Override
  public Channel create() throws IOException {
    return  conn.createChannel();
  }

  /**
   * Use the default PooledObject implementation.
   */
  @Override
  public PooledObject<Channel> wrap(Channel channel) {
    return new DefaultPooledObject<>(channel);
  }

  /**
   * When an object is returned to the pool, clear the buffer.
   */
  @Override
  public void passivateObject(PooledObject<Channel> pooledObject) throws IOException, TimeoutException {
    //pooledObject.getObject().close();
  }
  // for all other methods, the no-op implementation
  // in BasePooledObjectFactory will suffice
}