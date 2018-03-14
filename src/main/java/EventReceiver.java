import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class EventReceiver extends Thread{

    private static String RMQ_ENDPOINT;
    private static String queueName;
    private ConnectionFactory factory;
    private Connection connection;
    private Channel channel;
    private Consumer consumer;

    public EventReceiver(String endpoint){
        RMQ_ENDPOINT = endpoint;
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
                String message = new String(body, "UTF-8");
                System.out.println(" [x] Received '" + message + "'");
            }
        };

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
