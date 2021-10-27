package com.jdriven.mib.es;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.client.RequestOptions.DEFAULT;
import static org.elasticsearch.common.xcontent.XContentType.JSON;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;

public class CategoryItemRepository {
    public static final String INDEX_CAT_ITEM = "category-item";
    private final RestHighLevelClient client;
    private final ObjectMapper objectMapper;

    public static final String MAPPING = """
        {
            "mappings": {
                "properties": {
                    "id": {
                        "type": "keyword"
                    },
                    "main": {
                        "type": "keyword"
                    },
                    "sub": {
                        "type": "keyword"
                    }
                }
            }
        }
        """;

    public CategoryItemRepository(RestHighLevelClient client, ObjectMapper objectMapper) {
        this.client = client;
        this.objectMapper = objectMapper;
    }

    public void save(CategoryItem categoryItem) throws IOException {
        var request = new IndexRequest(INDEX_CAT_ITEM)
            .id(categoryItem.id())
            .source(objectMapper.writeValueAsString(categoryItem), JSON);

        client.index(request, DEFAULT);
    }

    public List<MainCategory> getCategoriesCount() throws IOException {
        var source = searchSource()
            .size(0) // return only the aggregations to avoid filling the aggregation cache
            .aggregation(
                terms("categories").field("main").size(50)
                    .subAggregation(
                        terms("subCategories").field("sub").size(100).order(BucketOrder.key(true))
                    )
            );

        var response = client.search(new SearchRequest(INDEX_CAT_ITEM).source(source), DEFAULT);

        List<MainCategory> results = new ArrayList<>();
        var categoriesTerms = response.getAggregations().<Terms>get("categories");
        categoriesTerms.getBuckets().forEach(categoryBucket -> {
            List<SubCategory> subCategories =
                categoryBucket.getAggregations().<Terms>get("subCategories")
                .getBuckets().stream()
                .map(subCategoryBucket ->
                    new SubCategory(subCategoryBucket.getKeyAsString(),
                        subCategoryBucket.getDocCount())
                ).toList();

            results.add(new MainCategory(categoryBucket.getKeyAsString(),
                categoryBucket.getDocCount(),
                subCategories));
        });

        return results;
    }

    public static record CategoryItem(String id, String main, String sub) {
    }

    public static record MainCategory(String category, long count, List<SubCategory> subCategories) {
    }

    public static record SubCategory(String category, long count) {
    }

}
