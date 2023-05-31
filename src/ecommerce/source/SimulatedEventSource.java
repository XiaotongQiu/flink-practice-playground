package ecommerce.source;

import ecommerce.model.MarketingUserBehavior;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class SimulatedEventSource extends RichParallelSourceFunction<MarketingUserBehavior> {
    Boolean running;
    List<String> channels;
    List<String> behaviours;
    Random rand;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        this.running = true;
        this.channels = new ArrayList<>();
        this.behaviours = new ArrayList<>();

        channels.add("XiaomiStore");
        channels.add("AppStore");

        behaviours.add("BROWSE");
        behaviours.add("CLICK");
        behaviours.add("UNINSTALL");

        rand = new Random();
    }

    @Override
    public void run(SourceContext<MarketingUserBehavior> sourceContext) throws Exception {
        while (running) {
            String userId = UUID.randomUUID().toString();
            String behavior = behaviours.get(rand.nextInt(behaviours.size()));
            String channel = channels.get(rand.nextInt(channels.size()));

            long ts = System.currentTimeMillis();

            sourceContext.collect(new MarketingUserBehavior(userId, behavior, channel, ts));

            Thread.sleep(10);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}
