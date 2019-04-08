package gov.baaqmd.kafka.connect.utils;

import java.io.File;
import java.io.FileFilter;

public class DirFilterWatcher implements FileFilter {
    private String filter;

    public DirFilterWatcher() {
        this.filter = "";
    }

    public DirFilterWatcher(String filter) {
        this.filter = filter;
    }

    public boolean accept(File file) {
        // System.out.println ("File " + file.getName() + " filter: " + this.filter);
        if ("".equals(filter)) {
            return true;
        } else if  (file.getName().endsWith("_MIN.dat")) {  // filter out MIN data, -jun
            return false;
        }
        // System.out.println ("File " + file.getName() + " action: SKIP...");
        return (file.getName().endsWith(filter));
    }
}