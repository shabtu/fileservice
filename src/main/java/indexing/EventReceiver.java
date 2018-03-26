package indexing;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.rabbitmq.client.*;
import common.ElasticsearchService;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class EventReceiver extends Thread{

    private static String RMQ_ENDPOINT;
    private static String queueName;
    private ConnectionFactory factory;
    private Connection connection;
    private Channel channel;
    private Consumer consumer;
    Gson gson;
    int id = 0;

    ElasticsearchService elasticsearchService;

    public EventReceiver(String endpoint){
        RMQ_ENDPOINT = endpoint;
        elasticsearchService = new ElasticsearchService("localhost");
    }

    public void createConnection() throws IOException, TimeoutException {
        factory = new ConnectionFactory();
        factory.setHost(RMQ_ENDPOINT);
        connection = factory.newConnection();
        channel = connection.createChannel();

        channel.exchangeDeclare("bucketevents", "fanout");
        queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, "bucketevents", "");

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
    }

    public Channel getChannel() {
        return channel;
    }

    public void initiateConsumer() {

        Channel channel = getChannel();
        consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                String jsonString = new String(body, "UTF-8");
                System.out.println(" [x] Received '" + jsonString + "'");


                AttachmentFile file = parseFile(jsonString);

                System.out.println(file.toString());
                indexFile(file);


            }
        };
    }

    public AttachmentFile parseFile(String jsonString){

        JsonObject jobj = new Gson().fromJson(jsonString, JsonObject.class);

        String[] bucketAndFilename = jobj.get("Key").toString().split("/");

        String[] fields = bucketAndFilename[1].split("_");


        return new AttachmentFile(Integer.parseInt(fields[0]), Integer.parseInt(fields[1]), fields[2], fields[3], fields[4].substring(0,fields[4].length()-2), bucketAndFilename[0].substring(1));
    }

    public void indexFile(AttachmentFile file) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("bo", file.sno);
            builder.field("bo_sno", file.bo_sno);
            builder.field("name", file.name);
            builder.field("uid", file.uniqueId);
            builder.field("date", file.creationDate);
            builder.field("bucket", file.bucket);
        }
        builder.endObject();
        IndexRequest indexRequest = new IndexRequest("invoices", "doc",  String.valueOf(id++))
                .source(builder);

        elasticsearchService.getElasticsearchClient().index(indexRequest);
    }

    public void startConsuming() throws IOException {
         channel.basicConsume(queueName, true, consumer);
    }

    @Override
    public void run() {
        try {
            startConsuming();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
