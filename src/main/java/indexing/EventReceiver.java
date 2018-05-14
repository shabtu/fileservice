package indexing;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.rabbitmq.client.*;
import common.FileInfo;
import common.ElasticsearchService;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class EventReceiver extends Thread{

    /*RabbitMQ endpoint and queue name which will be set later*/
    private static String RMQ_ENDPOINT;
    private static String queueName;

    /*Indexer class for counting indexed files through the atomic
      counter until all files are indexed*/
    private final Indexer indexer;

    private Channel channel;
    private Consumer consumer;

    private ElasticsearchService elasticsearchService =  new ElasticsearchService("localhost");

    public EventReceiver(String endpoint, Indexer indexer){
        RMQ_ENDPOINT = endpoint;
        this.indexer = indexer;
    }

    public void createConnection() throws IOException, TimeoutException {

        /*Establish connection to RabbitMQ*/
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RMQ_ENDPOINT);
        Connection connection = factory.newConnection();
        channel = connection.createChannel();

        /*Declare exchange and queue for RabbitMQ*/
        channel.exchangeDeclare("bucketevents", "fanout");
        queueName = channel.queueDeclare("minioevents", true, false, false, null).getQueue();
        channel.queueBind(queueName, "bucketevents", "");
    }

    private Channel getChannel() {
        return channel;
    }

    public void initiateConsumer() {


        /*Handle incoming messages from the RabbitMQ channel*/
        Channel channel = getChannel();
        consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                String jsonString = new String(body, "UTF-8");
               // System.out.println(" [x] Received '" + jsonString + "'");

                /*Parse the incoming JSON-object*/
                FileInfo file = parseFile(jsonString);

                /*Index the file meta-data into Elasticsearch*/
                indexFile(file);

                /*If all files are not indexed, continue indexing*/
                if (indexer.indexCounter.get() >= indexer.getNumberOfFiles())
                    return;
                else {
                    //System.out.println("Number of indexed files: " + indexer.indexCounter.get());
                    indexer.indexCounter.getAndIncrement();
                }
            }
        };
    }

    @Override
    public void run() {
        try {
            /*Start consuming messages from the declared queue*/
            startConsuming();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*Uses the Elasticsearch client to index the file using its info*/
    private void indexFile(FileInfo file) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();


        /*Input the correct fields for the entry to Elasticsearch*/
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

        /*Submit the index request to the Elasticsearch server*/
        elasticsearchService.getElasticsearchClient().index(indexRequest);
    }

    /*Start the thread*/
    private void startConsuming() throws IOException {
        channel.basicConsume(queueName, true, consumer);
    }

    /*File name parser*/
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
}
