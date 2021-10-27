package com.jdriven.mib.es;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.jdriven.mib.es.CategoryItemRepository.CategoryItem;
import com.jdriven.mib.es.CategoryItemRepository.MainCategory;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.Optional;

import static com.jdriven.mib.es.CategoryItemRepository.INDEX_CAT_ITEM;
import static com.jdriven.mib.es.CategoryItemRepository.MAPPING;
import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class CategoryItemRepositoryTest {

    private RestHighLevelClient client;
    private ObjectMapper objectMapper;

    @Container
    private static final ElasticsearchContainer container = new ElasticsearchContainer(
        DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch-oss").withTag("7.10.2")
    );

    @BeforeEach
    void setUp() throws IOException {
        client = new RestHighLevelClient(RestClient.builder(HttpHost.create(container.getHttpHostAddress())));

        if (client.indices().exists(new GetIndexRequest(INDEX_CAT_ITEM), RequestOptions.DEFAULT)) {
            client.indices().delete(new DeleteIndexRequest(INDEX_CAT_ITEM), RequestOptions.DEFAULT);
        }
        client.indices().create(new CreateIndexRequest(INDEX_CAT_ITEM).source(MAPPING, XContentType.JSON), RequestOptions.DEFAULT);
        objectMapper = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);
    }

    @Test
    void shouldReturnCategoriesCount() throws IOException {
        var repository = new CategoryItemRepository(client, objectMapper);

        repository.save(new CategoryItem("1", "main1", "sub1"));
        repository.save(new CategoryItem("2", "main1", "sub2"));
        repository.save(new CategoryItem("3", "main1", "sub2"));
        repository.save(new CategoryItem("4", "mainOnly", null));
        repository.save(new CategoryItem("5", "mainOnly", null));
        repository.save(new CategoryItem("6", "main2", "sub2"));
        repository.save(new CategoryItem("7", "main2", "sub2"));

        refreshIndex(); // force store, default documents are stored async

        var categories = repository.getCategoriesCount();

        assertThat(categories.stream().map(MainCategory::category)).containsExactly("main1", "main2", "mainOnly");
        assertThat(categories.stream().map(MainCategory::count)).containsExactly(3L, 2L, 2L);
        var maybeMain1 = categories.stream().filter(cat -> cat.category().equals("main1")).findFirst();
        assertThat(maybeMain1).isPresent();
        assertThat(maybeMain1.get().subCategories().stream()
            .filter(sub -> sub.category().equals("sub2"))
            .map(CategoryItemRepository.SubCategory::count)).containsExactly(2L);
    }

    private void refreshIndex() throws IOException {
        client.indices().refresh(new RefreshRequest(INDEX_CAT_ITEM), RequestOptions.DEFAULT);
    }

    @Test
    void shouldBeHealthy() throws IOException {
        Response response = client.getLowLevelClient().performRequest(new Request("GET", "/_cluster/health"));
        assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);
    }
}