package mongo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.extern.log4j.Log4j;
import mongo.conn.MongoDBConnection;
import mongo.dto.AccessLogDTO;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Log4j
public class MonthLogReducer2 extends Reducer<Text, Text, Text, IntWritable> {

    private MongoDatabase mongodb;

    private Map<String, String> months = new HashMap<>();

    public MonthLogReducer2() {
        this.months.put("Jan", "Log_01");
        this.months.put("Feb", "Log_02");
        this.months.put("Mar", "Log_03");
        this.months.put("Apr", "Log_04");
        this.months.put("May", "Log_05");
        this.months.put("Jun", "Log_06");
        this.months.put("Jul", "Log_07");
        this.months.put("Aug", "Log_08");
        this.months.put("Sep", "Log_09");
        this.months.put("Oct", "Log_10");
        this.months.put("Nov", "Log_11");
        this.months.put("Dec", "Log_12");
    }
    @Override
    protected void setup(Reducer<Text, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        this.mongodb = new MongoDBConnection().getMongodb();

        for (String month : this.months.keySet()) {
            String colNm = this.months.get(month);
            boolean create = true;

            for (String s : this.mongodb.listCollectionNames()) {
                if (colNm.equals(s)) {
                    create = false;
                    break;
                }
            }
            if (create) {
                this.mongodb.createCollection(colNm);
            }
        }
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        List<Document> pList = new ArrayList<>();

        String colNm = this.months.get(key.toString());

        log.info("key : "+key);
        log.info("colNm : "+colNm);

        MongoCollection<Document> col = mongodb.getCollection(colNm);

        for (Text value : values) {
            String json = value.toString();

            AccessLogDTO pDTO = new ObjectMapper().readValue(json, AccessLogDTO.class);
            Document doc = new Document(new ObjectMapper().convertValue(pDTO, Map.class));
            pList.add(doc);
            doc = null;

        }
        int pListSize = pList.size();
        int blockSize = 50000;
        for (int i = 0; i < pListSize; i += blockSize) {
            col.insertMany(new ArrayList<>(pList.subList(i, Math.min(i+blockSize, pListSize))));
        }

        col = null;
        pList = null;
        }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        this.mongodb = null;
    }
}
