package ar.edu.itba.pod.client.queries;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.mapreduce.JobTracker;
import org.apache.commons.cli.CommandLine;

import java.io.FileWriter;
import java.io.IOException;

public abstract class BaseQuery implements Query {
    protected final HazelcastInstance hazelcastInstance;
    private final CommandLine arguments;

    public BaseQuery(final HazelcastInstance hazelcastInstance, final CommandLine arguments) {
        this.hazelcastInstance = hazelcastInstance;
        this.arguments = arguments;
    }

    @Override
    public void writeResult() throws IOException {
        final FileWriter fileWriter = new FileWriter(arguments.getOptionValue("outPath"));
        fileWriter.write(getResult());
        fileWriter.close();
    }

    public JobTracker getJobTracker() {
        return hazelcastInstance.getJobTracker("g3_" + "query" + arguments.getOptionValue("query"));
    }
}
