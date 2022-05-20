package mongo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.extern.log4j.Log4j;
import mongo.conn.MongoDBConnection;
import mongo.dto.AccessLogDTO;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.Document;

import java.io.IOException;
import java.util.Map;

@Log4j
public class AccessLogMapper extends Mapper<LongWritable, Text, Text, Text> {

    private MongoDatabase mongodb;

    private String colNm = "ACCESS_LOG";

    @Override
    public void setup(Mapper<LongWritable, Text, Text,Text>.Context context) throws IOException, InterruptedException {
        this.mongodb = new MongoDBConnection().getMongodb();

        boolean create = true;

        for (String s : this.mongodb.listCollectionNames()) {

            if (this.colNm.equals(s)) {
                create = false;
                break;
            }
        }
        if (create) {
            this.mongodb.createCollection(this.colNm);
        }
    }
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        String[] fields = value.toString().split(" ");
        String ip = fields[0];
        String reqTime = "";

        if (fields[3].length() > 2) {
            reqTime = fields[3].substring(1);
        }
        String reqMethod = "";

        if (fields[5].length() > 2) {
            reqMethod = fields[5].substring(1);
        }
        String reqURI = fields[6];
        log.info("ip : " + ip);
        log.info("reqTime : " + reqTime);
        log.info("reqMethod : " + reqMethod);
        log.info("reqURI : " + reqURI);

        AccessLogDTO pDTO = new AccessLogDTO();
        pDTO.setIp(ip);
        pDTO.setReqMethod(reqMethod);
        pDTO.setReqURI(reqURI);
        pDTO.setReqTime(reqTime);

        MongoCollection<Document> col = this.mongodb.getCollection(this.colNm);
        col.insertOne(new Document(new ObjectMapper().convertValue(pDTO, Map.class)));

        col = null;
    }

    @Override
    protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException{
        this.mongodb = null;
    }
}
