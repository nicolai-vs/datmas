package generator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

public class PhoneNumbsPrepro {
    public static void main(String[] args) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader("src/main/data/1000RandPhoneNumbs.txt"));
        PrintWriter writer = new PrintWriter("src/main/data/1000FormattedPhoneNumbs.txt", "UTF-8" );
        String line;
        while ((line = reader.readLine()) != null){
            line = formatString(line);
            System.out.println(line);
            writer.println(line);
        }
        writer.close();
        reader.close();
    }
    private static String formatString(String line){
        return line.replace(" ", "")
                .replace("(", "")
                .replace(")", "")
                .replace("-", "");
    }
}

