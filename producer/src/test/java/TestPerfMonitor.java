import com.aiven.test.commons.ContextProvider;
import com.aiven.test.commons.service.KafkaService;
import com.aiven.test.producer.service.PerformanceMonitor;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class TestPerfMonitor {
    @Before
    public void setUp() throws IOException {
        ContextProvider.init();
    }

    @Test
    public void testPerfMonitorSystemInfo() {
        KafkaService kafkaService = new KafkaService();
        PerformanceMonitor monitor = new PerformanceMonitor(kafkaService);
        String si = monitor.getSystemInfo();
        JSONObject o = new JSONObject(si);
        assertTrue(o.has("systemId"));
    }


}
