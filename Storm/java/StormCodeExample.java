public class Spout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private BufferedReader in;
    public void open(Map map, TopologyContext ctx, SpoutOutputCollector collector) {
        this.collector = collector;
        String host = "localhost";
        int port = 9999;
        try {
            Socket s = new Socket(host, port);
            in = new BufferedReader(
                    new InputStreamReader(s.getInputStream()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void nextTuple() {
        try {
            String jsonString = in.readLine();

            if (jsonString == null) {
                throw new SocketException();
            }

            collector.emit(new Values(jsonString));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void declareOutputFields(OutputFieldsDeclarer decl) {
        decl.declare(new Fields("field"));
    }
}
public class Bolt extends BaseBasicBolt {
    private int counter = 1;
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String text = tuple.getString(0);
        basicOutputCollector.emit(new Values(text));
        System.out.println(String.valueOf(counter) + ": " + text);
        counter++;
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("field"));
    }
}

public class Topology {
    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Spout", new Spout());
        builder.setBolt("Bolt", new Bolt()).shuffleGrouping("Spout");

        Config config = new Config();
        config.setNumWorkers(20);
        config.setMaxSpoutPending(5000);

        StormSubmitter stormSubmitter = new StormSubmitter();
        try {
            stormSubmitter.submitTopology("simulation", config, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}





