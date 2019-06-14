import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.SocketException;
import java.util.Map;

public class IntegerSpout extends BaseRichSpout {
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
                // make the topology die
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
