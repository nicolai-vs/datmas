import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class MainTopology {
    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("IntegerSpout", new IntegerSpout());
        builder.setBolt("MultiplierBolt", new MultiplierBolt()).shuffleGrouping("IntegerSpout");

        Config config = new Config();
        // config.setDebug(true);

        // LocalCluster cluster = new LocalCluster();
        // try {
        //     cluster.submitTopology("HelloTopology", config, builder.createTopology());
            // Thread.sleep(20000);
        // }catch (Exception e) {
        //     e.printStackTrace();
        // }

        config.setNumWorkers(20);
        config.setMaxSpoutPending(5000);

        StormSubmitter stormSubmitter = new StormSubmitter();
        try {
            stormSubmitter.submitTopology("MyTopology", config, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
        }




    }
}
