package ar.edu.itba.pod.server;

import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class Server {
    private static Logger logger = LoggerFactory.getLogger(Server.class);

    public static void main(String[] args) {
        logger.info("Server Starting ...");

        /*String addresses = Optional.ofNullable(System.getProperty("addresses")).orElseThrow(IllegalArgumentException::new);
        String addressList[] = addresses.split(";");

        Config config = new Config();
        NetworkConfig networkConfig = config.getNetworkConfig();

        for (String address : addressList) {
        }*/

        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
    }
}
