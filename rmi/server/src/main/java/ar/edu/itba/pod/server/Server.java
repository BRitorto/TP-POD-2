package ar.edu.itba.pod.server;

import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Server {
    private static Logger logger = LoggerFactory.getLogger(Server.class);
    private static final String ADDRESSES_NAME = "addresses";
    private static final String ADDRESSES_DESCRIPTION = "Address to start the server on";

    public static void main(String[] args) {
        logger.info("rmi Server Starting ...");
        Options options;
        options = new Options();
        addOption(options, ADDRESSES_NAME, ADDRESSES_DESCRIPTION, true, true);
        parse(args, options);
        Config serverConfig = new Config()
                .setGroupConfig(new GroupConfig()
                        .setName("g3")
                        .setPassword("g3")
                )
                .setNetworkConfig(new NetworkConfig()
                        .setJoin(new JoinConfig()
                                .setMulticastConfig(new MulticastConfig()
                                        .setEnabled(false))
                                .setTcpIpConfig(new TcpIpConfig()
                                        .setEnabled(true)
                                        .setMembers(options.getOption("addresses").getValuesList())
                                )
                        )
                );

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(serverConfig);
    }

    public static CommandLine parse(final String[] args, final Options options){
        CommandLineParser parser = new DefaultParser();

        try {
            return parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println( "Parsing failed.  Reason: " + e.getMessage());
            System.exit(1);
            return null;
        }
    }

    private static void addOption(Options options, final String name, final String description, final boolean hasArguments, final boolean required) {
        Option op = new Option(name, name, hasArguments, description);
        op.setRequired(required);
        options.addOption(op);
    }
}
