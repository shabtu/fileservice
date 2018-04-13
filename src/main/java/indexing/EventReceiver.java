package indexing;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.rabbitmq.client.*;
import common.FileInfo;
import common.ElasticsearchService;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class EventReceiver extends Thread{

    private static String RMQ_ENDPOINT;
    private static String queueName;
    private Channel channel;
    private Consumer consumer;
    private int id = 0;

    private ElasticsearchService elasticsearchService =  new ElasticsearchService("localhost");

    public EventReceiver(String endpoint){
        RMQ_ENDPOINT = endpoint;
    }

    public void createConnection() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RMQ_ENDPOINT);
        Connection connection = factory.newConnection();
        channel = connection.createChannel();

        channel.exchangeDeclare("bucketevents", "fanout");
        queueName = channel.queueDeclare("minioevents", true, false, false, null).getQueue();
        channel.queueBind(queueName, "bucketevents", "");

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
    }

    private Channel getChannel() {
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
                System.out.println("To queue: " + queueName);

                FileInfo file = parseFile(jsonString);

                indexFile(file);
            }
        };
    }

    private FileInfo parseFile(String jsonString){

        JsonObject jobj = new Gson().fromJson(jsonString, JsonObject.class);

        String[] fields = jobj.get("Key").toString().split("/");

        return new FileInfo(Integer.parseInt(fields[1]),
                Integer.parseInt(fields[2]),
                fields[3],
                fields[4],
                fields[5],
                fields[0]);
    }

    private void indexFile(FileInfo file) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("sno", file.getSno());
            builder.field("bo_sno", file.getBo_sno());
            builder.field("name", file.getName());
            builder.field("uid", file.getUniqueId());
            builder.field("date", file.getCreationDate());
            builder.field("bucket", file.getBucket());
        }
        builder.endObject();
        IndexRequest indexRequest = new IndexRequest("invoices", "doc",  file.getUniqueId())
                .source(builder);

        elasticsearchService.getElasticsearchClient().index(indexRequest);
    }

    private void startConsuming() throws IOException {
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
