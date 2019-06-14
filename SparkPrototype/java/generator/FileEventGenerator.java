package generator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class FileEventGenerator {

    @SuppressWarnings("Duplicates")
    public static void main(String[] args) throws IOException, InterruptedException {
        String fileName = "src/main/data/OLD_phoneEvents";
        Double eventsPRSecond = 100.0;
        Double threadSleepMillisDouble = (1/eventsPRSecond) * 1000.0;
        int threadSleepMillis = (int)Math.round(threadSleepMillisDouble);
        System.out.println("ThreadSleepMillis: " + threadSleepMillis);

        TcpServer server = new TcpServer();
        server.ServerAcceptor(9998);
        System.out.println("Connection Established");
        String line;
        FileReader fileReader = new FileReader(fileName);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        int counter = 1;
        while ((line = bufferedReader.readLine()) != null){
            server.Send(line);
            System.out.println(counter++);
            Thread.sleep(threadSleepMillis);

        }



    }

}