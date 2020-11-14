package myhighLevelClient;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Map;

public class ESOperateWithHighLevelClient {

    RestHighLevelClient client = null;

    public void initES() {
//        client = new RestHighLevelClient(
//                RestClient.builder(
//                        new HttpHost("node01", 9200, "http"),
//                        new HttpHost("node02", 9200, "http"),
//                        new HttpHost("node03", 9200, "http")));


        RestClientBuilder restClient = RestClient.builder(
                new HttpHost("node01", 9200, "http"));
        client = new RestHighLevelClient(restClient);


    }

    public void closeES() {
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void searchAll(){
        this.initES();
        SearchRequest searchRequest = new SearchRequest("document");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        try {
            final SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits hits = searchResponse.getHits();
            for (SearchHit hit: hits) {
//                 System.out.println(hit.getSourceAsString());
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                System.out.printf("Id: %s, Title: %s\n", hit.getId(), sourceAsMap.get("title"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.closeES();
    }

    public void searchByKey(String key) {
        this.initES();
        SearchRequest searchRequest = new SearchRequest("document");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("author", key));
        searchRequest.source(searchSourceBuilder);
        try {
            final SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits hits = searchResponse.getHits();
            for (SearchHit hit: hits) {
//                 System.out.println(hit.getSourceAsString());
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                System.out.printf("Id: %s, Title: %s\n", hit.getId(), sourceAsMap.get("title"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.closeES();
    }

    public void searchByRange(String begin, String end) {
        this.initES();
        SearchRequest searchRequest = new SearchRequest("school");
        searchRequest.types("student");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.rangeQuery("age").from(begin).to(end));
        searchRequest.source(searchSourceBuilder);
        try {
            final SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits hits = searchResponse.getHits();
            for (SearchHit hit: hits) {
//                 System.out.println(hit.getSourceAsString());
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                System.out.printf("Id: %s, Age: %s\n", hit.getId(), sourceAsMap.get("age"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.closeES();
    }

    /**
     * 原因分析
     * https://www.elastic.co/guide/en/elasticsearch/reference/current/fielddata.html
     *
     * text类型的字段在查询时使用的是在内存中的称为fielddata的数据结构。这种数据结构是在第一次将字段用于聚合/排序/脚本时基于需求建立的。
     *
     * 它通过读取磁盘上每个segmet上所有的倒排索引来构建，反转term和document的关系(倒排)，并将结果存在Java堆上(内存中)。(因此会耗费很多的堆空间，特别是在加载很高基数的text字段时)。一旦fielddata被加载到堆中，它在segment中的生命周期还是存在的。
     *
     * 因此，加载fielddata是一个非常消耗资源的过程，甚至能导致用户体验到延迟.这就是为什么 fielddata 默认关闭。
     *
     * 开启text 字段的fielddata
     *
     * PUT school/_mapping/student/
     * {
     *   "properties": {
     *     "name": {
     *       "type":     "text",
     *       "fielddata": true
     *     }
     *   }
     * }
     * @param text
     */
    public void searchFuzzy(String text) {
        this.initES();
        SearchRequest searchRequest = new SearchRequest("school");
        searchRequest.types("student");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.fuzzyQuery("about", text));
        searchSourceBuilder.sort("name", SortOrder.ASC);
        searchRequest.source(searchSourceBuilder);
        try {
            final SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits hits = searchResponse.getHits();
            for (SearchHit hit: hits) {
//                 System.out.println(hit.getSourceAsString());
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                System.out.printf("Id: %s, Name: %s\n", hit.getId(), sourceAsMap.get("name"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.closeES();
    }

    public void searchFuzzyWithOrderByPage(String text) {
        this.initES();
        SearchRequest searchRequest = new SearchRequest("school");
        searchRequest.types("student");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.fuzzyQuery("about", text));
        searchSourceBuilder.sort("name", SortOrder.ASC);
        searchSourceBuilder.from(2).size(2);
        searchRequest.source(searchSourceBuilder);
        try {
            final SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits hits = searchResponse.getHits();
            for (SearchHit hit: hits) {
//                 System.out.println(hit.getSourceAsString());
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                System.out.printf("Id: %s, Name: %s\n", hit.getId(), sourceAsMap.get("name"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.closeES();
    }

    public void searchFuzzyWithHighLightByPage(String text) {
        this.initES();
        SearchRequest searchRequest = new SearchRequest("school");
        searchRequest.types("student");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.fuzzyQuery("about", text));
        searchSourceBuilder.sort("_id", SortOrder.ASC);
        searchSourceBuilder.from(2).size(2);
        searchSourceBuilder.highlighter(new HighlightBuilder().field("about").preTags("<font style='color:red'>").postTags("</font>"));
        searchRequest.source(searchSourceBuilder);
        try {
            final SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits hits = searchResponse.getHits();
            for (SearchHit hit: hits) {
                Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                //System.out.println(highlightFields.size());
                for (Text highLight: highlightFields.get("about").getFragments()) {
                    System.out.println("highLight: " + highLight);
                }
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                System.out.printf("Id: %s, Name: %s\n", hit.getId(), sourceAsMap.get("name"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.closeES();
    }

    public static void main(String[] args) {
        ESOperateWithHighLevelClient esOperate =  new ESOperateWithHighLevelClient();
//        esOperate.searchAll();
//        esOperate.searchByKey("gongbailiang");
//        esOperate.searchByRange("15", "20");
        esOperate.searchFuzzy("travel");
        System.out.println("============================================");
        esOperate.searchFuzzyWithOrderByPage("travel");

//        esOperate.searchFuzzyWithHighLightByPage("travle");
    }
}
