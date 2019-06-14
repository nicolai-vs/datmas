package StreamProcessor;

import org.apache.spark.sql.SparkSession;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

public class QueryListener {
    private SparkSession spark;

    public QueryListener(SparkSession spark){
        this.spark = spark;
    }


    public void Server(int port){
        Thread thread = new Thread(() ->{
            try {
                ServerSocket server = new ServerSocket(port);
                Thread.sleep(10);
                Socket socket = server.accept();


                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                JSONObject object = new JSONObject(in.readLine());
                System.out.println(object + " --- " +  object.toString());


                // Find which stream it is and execute the request.
                // Must be able to create a new stream that needs:
                    // host, port, sample/schema, TableName and Query.






                socket.close();

            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        thread.start();
    }

}
