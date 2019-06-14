import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class MultiplierBolt extends BaseBasicBolt {
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
