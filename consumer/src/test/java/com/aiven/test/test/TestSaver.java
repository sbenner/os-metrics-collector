package com.aiven.test.test;

import com.aiven.test.commons.model.Metric;
import com.aiven.test.consumer.converter.MetricConverter;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertNotNull;

@RunWith(JUnit4.class)
public class TestSaver {
    @Test
    public void testConverter() {
        MetricConverter converter = new MetricConverter();
        String data = "{\"systemId\":\"AFC1FBFF009006EA\",\"temperature\":40,\"ts\":1600856440401}";
        JSONObject o = new JSONObject(data);
        Metric m = converter.toObject(o);
        assertNotNull(m.getSystemId());
    }


}
