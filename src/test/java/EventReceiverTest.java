import indexing.EventReceiver;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

class EventReceiverTest {


    static EventReceiver eventReceiver;
    public static final String RMQ_ENDPOINT = "10.12.97.194";

    @BeforeEach
    void init() throws IOException, TimeoutException {
        eventReceiver = new EventReceiver(RMQ_ENDPOINT);
        eventReceiver.createConnection();
        eventReceiver.initiateConsumer();
    }
}
