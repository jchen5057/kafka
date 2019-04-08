package gov.baaqmd.kafka.connect.directory;

import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import gov.baaqmd.kafka.connect.utils.DirWatcher;
import gov.baaqmd.kafka.connect.utils.ReadDatFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.*;


/**
 * DirectorySourceTask is a Task that reads changes from a directory for storage
 * new binary detected files in Kafka.
 *
 */
public class DirectorySourceTask extends SourceTask {
    private final static Logger log = LoggerFactory.getLogger(DirectorySourceTask.class);
    private final static String LAST_UPDATE = "last_update";
    private final static String FILE_UPDATE = "file_update";

    private String tmp_path;

    private TimerTask task;
    private static Schema schema = null;
    private String schema_cfg;
    private String topic_cfg;
    private String filter_cfg;
    private String timer_cfg;
    Long offset = null;
    private Set<File> retries = new HashSet<>();


    @Override
    public String version() {
        return new DirectorySourceConnector().version();
    }

    /**
     * Start the Task. Handles configuration parsing and one-time setup of the Task.
     *
     * @param props initial configuration
     */
    @Override
    public void start(Map<String, String> props) {
        schema_cfg = props.get(DirectorySourceConnector.SCHEMA_CFG);
        if (schema_cfg == null)
            throw new ConnectException("config schema.name null");
        topic_cfg = props.get(DirectorySourceConnector.TOPIC_CFG);
        if (topic_cfg == null)
            throw new ConnectException("config topic_cfg null");

        filter_cfg = props.get(DirectorySourceConnector.FILTER_CFG);
        if (filter_cfg == null)
            throw new ConnectException("config filter_cfg null");

        tmp_path = props.get(DirectorySourceConnector.PATH_CFG);
        if (tmp_path == null)
            throw new ConnectException("config tmp.path null");

        timer_cfg = props.get(DirectorySourceConnector.TIMER_CFG);

        loadOffsets();

        task = new DirWatcher(context.offsetStorageReader(), tmp_path, filter_cfg, offset == null ? null : FileTime.fromMillis(offset)) {
            protected void onChange(File file, String action) {
                // here we code the action on a change
                System.out.println ("File " + file.getName() + " action: " + action);
            }
        };
        Timer timer = new Timer();
        timer.schedule(task, new Date(), Long.parseLong(timer_cfg));

        log.trace("Creating schema");
        schema = SchemaBuilder
                .struct()
                .name(schema_cfg)
                .field("id", Schema.OPTIONAL_STRING_SCHEMA)
                .field("station", Schema.OPTIONAL_STRING_SCHEMA)
                .field("event", Schema.OPTIONAL_STRING_SCHEMA)
                .field("state", Schema.OPTIONAL_STRING_SCHEMA)
                // .field("date", Schema.OPTIONAL_STRING_SCHEMA)
                .field("data", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

    /**
     * Poll this DirectorySourceTask for new records.
     *
     * @return a list of source records
     * @throws InterruptedException
     */
    @Override
    public List<SourceRecord> poll() throws InterruptException {

        List<SourceRecord> records = new ArrayList<>();
        Queue<File> queue = ((DirWatcher) task).getFilesQueue();

        // consume here the pool
        while (!queue.isEmpty()) {
            File file = queue.poll();
            String fname = file.getName();
            if (!fname.contains("_MIN.") && fname.endsWith(filter_cfg)) {
                try {
                    records.addAll(createUpdateRecord(file));
                } catch(Exception ex) {
                    //ex.printStackTrace();
                    retries.add(file);
                    records.add(createPendingRecord(file));
                }    
            }
        }
        if (retries.size() > 0) {
            queue.addAll(retries);
            retries.clear();
        }

        return records;
    }

    private SourceRecord createPendingRecord(File file) {
        String[] site = file.getName().split("_");
        String id = site[0];
        String station = site[1].replace(".dat", "");
        // creates the structured message
        Struct messageStruct = new Struct(schema);
        messageStruct.put("id", id);
        messageStruct.put("station", station);
        messageStruct.put("event", "pending");
        messageStruct.put("state", "pending");
        return new SourceRecord(Collections.singletonMap(file.toString(), "state"), Collections.singletonMap("pending", "yes"), topic_cfg, messageStruct.schema(), messageStruct);
    }

    /**
     * Create a new SourceRecord from a File
     *
     * @return a source records
     */
    private List<SourceRecord> createUpdateRecord(File file) {
        List<SourceRecord> recs = new ArrayList<>();

        String[] site = file.getName().split("_");
        String id = site[0];
        String station = site[1].replace(".dat", "");
        // creates the structured message
        Struct messageStruct = new Struct(schema);
        messageStruct.put("id", id);
        messageStruct.put("station", station);
        messageStruct.put("event", "committed");
        messageStruct.put("state", "pending");
        recs.add(new SourceRecord(Collections.singletonMap(file.toString(), "state"), Collections.singletonMap("committed", "yes"), topic_cfg, messageStruct.schema(), messageStruct));

        try {
            BasicFileAttributes fa = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
            FileTime lastMod = fa.lastModifiedTime();
            FileTime created = fa.lastAccessTime();
            List<String> data = ReadDatFile.readMetData(file);

            data.forEach((v) ->{
                Struct dataStruct = new Struct(schema);
                dataStruct.put("id", id);
                dataStruct.put("station", station);
                dataStruct.put("event", lastMod.compareTo(created) <= 0 ? "CREATED" : "MODIFIED");
                dataStruct.put("state", "committed");
                // dataStruct.put("date", k);
                dataStruct.put("data", v);
                // creates the record
                // no need to save offsets
                recs.add(new SourceRecord(offsetKey(), offsetValue(lastMod.compareTo(created) > 0 ? lastMod : created), topic_cfg, dataStruct.schema(), dataStruct));    
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
        return recs;
    }

    private Map<String, String> offsetKey() {
        return Collections.singletonMap(FILE_UPDATE, tmp_path);
    }

    private Map<String, Object> offsetValue(FileTime time) {
        offset = time.toMillis();
        return Collections.singletonMap(LAST_UPDATE, offset);
    }

    /**
     * Loads the current saved offsets.
     */
    private void loadOffsets() {
        Map<String, Object> off = context.offsetStorageReader().offset(offsetKey());
        if (off != null)
            offset = (Long) off.get(LAST_UPDATE);
    }

    /**
     * Signal this SourceTask to stop.
     */
    @Override
    public void stop() {
        task.cancel();
    }
}