package dev.berke.customer_data.customer;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.ScrollResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import dev.berke.customer_data.utils.Utils;
import jakarta.annotation.PostConstruct;
import org.apache.http.HttpHost;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.net.ssl.SSLContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class ExtractCustomers {

    private static final Logger logger = LoggerFactory.getLogger(ExtractCustomers.class);
    private final Utils utils;

    public ExtractCustomers(Utils utils) {
        this.utils = utils;
    }

    @PostConstruct
    public void extractAndIndexCustomers() throws Exception {
        Map<Integer, String> processedCustomers = new HashMap<>();
        logger.info("Starting customer extraction from 'kibana_sample_data_ecommerce'");

        var credentialsProvider = utils.createCredentialsProvider();
        SSLContext sslContext = Utils.getSSLContext();

        try (RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200, "https"))
                .setRequestConfigCallback(configBuilder -> configBuilder.setConnectTimeout(5000)
                        .setSocketTimeout(120000))
                .setHttpClientConfigCallback(builder -> builder.setSSLContext(sslContext)
                        .setDefaultCredentialsProvider(credentialsProvider)
                        .setKeepAliveStrategy((response, context) -> 120000)
                        .setMaxConnTotal(100)
                        .setMaxConnPerRoute(100)
                        .setDefaultIOReactorConfig(
                                IOReactorConfig.custom().setSoKeepAlive(true).build()))
                .build()) {

            RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
            ElasticsearchClient client = new ElasticsearchClient(transport);

            SearchRequest searchRequest = SearchRequest.of(s -> s
                    .index("kibana_sample_data_ecommerce")
                    .scroll(Time.of(t -> t.time("1m")))
                    .size(1000)
                    .query(q -> q.matchAll(m -> m)));

            SearchResponse<Map> searchResponse = client.search(searchRequest, Map.class);
            String scrollId = searchResponse.scrollId();
            List<Hit<Map>> hits = searchResponse.hits().hits();

            int processedCount = 0;

            while (hits != null && !hits.isEmpty()) {
                for (Hit<Map> hit : hits) {
                    Map<String, Object> sourceMap = hit.source();

                    if (sourceMap == null) {
                        continue;
                    }
                    Object idObj = sourceMap.get("customer_id");
                    Object emailObj = sourceMap.get("email");

                    if (idObj == null || emailObj == null || !(emailObj instanceof String)) {
                        logger.warn("Skipping document: invalid customer_id or email. Doc ID: {}",
                                hit.id());
                        continue;
                    }

                    Integer customerId = ((Number) idObj).intValue();
                    String currentEmail = (String) emailObj;

                    if (currentEmail.isBlank()) {
                        logger.warn("Skipping document: blank email. customer_id {} " +
                                "Doc ID: {}", customerId, hit.id());
                        continue;
                    }

                    if (processedCustomers.containsKey(customerId)) {
                        continue;
                    }

                    processedCustomers.put(customerId, currentEmail);

                    Map<String, Object> customerDoc = new HashMap<>();
                    customerDoc.put("customer_id", customerId);
                    customerDoc.put("customer_full_name", sourceMap.get("customer_full_name"));
                    customerDoc.put("customer_first_name", sourceMap.get("customer_first_name"));
                    customerDoc.put("customer_last_name", sourceMap.get("customer_last_name"));
                    customerDoc.put("customer_gender", sourceMap.get("customer_gender"));
                    customerDoc.put("email", currentEmail);
                    customerDoc.put("customer_phone", sourceMap.getOrDefault("customer_phone", ""));
                    customerDoc.put("user", sourceMap.get("user"));
                    client.index(i -> i
                            .index("customer")
                            .id(String.valueOf(customerId))
                            .document(customerDoc));
                    processedCount++;
                    if (processedCount % 500 == 0) {
                        logger.info("Indexed {} unique customers", processedCount);
                    }
                }

                final String currentScrollId = scrollId;

                ScrollResponse<Map> scrollResponse = client.scroll(s -> s
                        .scrollId(currentScrollId)
                        .scroll(Time.of(t -> t.time("1m"))), Map.class);
                scrollId = scrollResponse.scrollId();
                hits = scrollResponse.hits().hits();
            }

            logger.info("Customer extraction finished. Total unique customers indexed: {}", processedCount);

            final String finalScrollId = scrollId;
            if (finalScrollId != null) {
                client.clearScroll(c -> c.scrollId(finalScrollId));
            }
        }
    }
}