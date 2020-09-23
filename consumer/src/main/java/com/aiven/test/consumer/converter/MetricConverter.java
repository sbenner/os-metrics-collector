package com.aiven.test.consumer.converter;

import com.aiven.test.commons.model.Metric;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONObject;


public class MetricConverter {

    static final ObjectMapper objectMapper = new ObjectMapper();


    public Metric toObject(JSONObject dto) {

        Metric m = null;
        try {
            m = objectMapper.readValue(dto.toString(), Metric.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return m;
    }

}
