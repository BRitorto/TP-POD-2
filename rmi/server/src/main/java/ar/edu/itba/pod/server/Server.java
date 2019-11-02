package ar.edu.itba.pod.server;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import java.io.FileNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Server {
    private static Logger logger = LoggerFactory.getLogger(Server.class);

    public static void main(String[] args) throws FileNotFoundException {
        logger.info("rmi Server Starting ...");
        final Config serverConfig = new XmlConfigBuilder(
                "hazelcast.xml")
                .build();
        Hazelcast.newHazelcastInstance(serverConfig);
    }
}