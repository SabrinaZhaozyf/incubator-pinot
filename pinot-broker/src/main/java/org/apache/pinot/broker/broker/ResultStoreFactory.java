package org.apache.pinot.broker.broker;
import org.apache.pinot.broker.api.ResultStore;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ResultStoreFactory {
  public static final Logger LOGGER = LoggerFactory.getLogger(ResultStoreFactory.class);
  public static final String RESULT_STORE_CLASS_CONFIG = "class";

  public void init(PinotConfiguration configuration) {
  };

  public abstract ResultStore create();

  public static ResultStoreFactory loadFactory(PinotConfiguration configuration) {
    String resultStoreFactoryClassName = configuration.getProperty(RESULT_STORE_CLASS_CONFIG);
    ResultStoreFactory resultStoreFactory;
    try {
      resultStoreFactory = (ResultStoreFactory) Class.forName(resultStoreFactoryClassName).newInstance();
      resultStoreFactory.init(configuration);
      return resultStoreFactory;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
