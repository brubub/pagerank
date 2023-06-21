package edu.brunobudris.sdcc.pagerank.io;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
@Slf4j
public class HttpGraphProvider {

    @Value("${graph.url}")
    private String graphUrl;

    public byte[] provide() {
        final var template = new RestTemplate();
        ResponseEntity<byte[]> result = template.exchange(graphUrl, HttpMethod.GET, null, byte[].class);
        final var data = result.getBody();

        if (data == null || data.length == 0) {
            throw new RuntimeException("Data not retrieved for url: " + graphUrl);
        }
        log.info("Graph data content size: {} byte", data.length);

        return data;
    }
}