package gov.baaqmd.kafka.connect.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

/**
 * Read metPush data: line by line
 * Jun Chen 4/2/2019 Java8
 */
public class ReadDatFile {
    static DateTimeFormatter y4m2d2 = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss", Locale.ENGLISH);
    static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static List<String> readMetData(File file) {
        List<String> data = new ArrayList<String>();

        try (Stream<String> stream = Files.lines(Paths.get(file.getPath()))) {
            stream.forEach(s -> data.add(s));
            /*
            stream.forEach(s -> {
                try {
                    String[] d = s.split(",");
                    int yy = Integer.parseInt(d[2]);
                    int dd = Integer.parseInt(d[3]);
                    int hh = Integer.parseInt(d[4]) / 100;

                    LocalDateTime ldt = LocalDateTime.of(yy, 1, 1, 0, 0, 0).plusDays(dd).plusHours(hh);
                    String dt = y4m2d2.format(ldt);

                    data.put(dt, d[6]);
                } catch (Exception ex) {
                    // ex.printStackTrace();
                }
            });
            */
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return data;
    }

    public static boolean isDate(String s){
        // String pattern= "([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2}):([0-9]{2}):([0-9]{2})";
        // return s.matches(pattern);
        s = s.replaceAll("^\"|\"$", "");
        dateFormat.setLenient(false);
        try {
            dateFormat.parse(s.trim());
        } catch (ParseException pe) {
            return false;
        }
        return true;
    }
}