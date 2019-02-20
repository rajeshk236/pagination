package com.sample.pagination;


import com.microsoft.azure.cosmosdb.ConnectionPolicy;
import com.microsoft.azure.cosmosdb.ConsistencyLevel;
import com.microsoft.azure.cosmosdb.Document;
import com.microsoft.azure.cosmosdb.FeedOptions;
import com.microsoft.azure.cosmosdb.FeedResponse;
import com.microsoft.azure.cosmosdb.rx.AsyncDocumentClient;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import rx.Observable;
import rx.observers.TestSubscriber;

/**
 * Hello world!
 *
 */
public class Pagination
{
    private AsyncDocumentClient documentClient;
    private String collectionId="incident";
    private String databaseId="incident-management";


    public Pagination() {
        this.documentClient = new AsyncDocumentClient.Builder().withServiceEndpoint("Url").withMasterKeyOrResourceToken("Token")
                .withConnectionPolicy(ConnectionPolicy.GetDefault()).withConsistencyLevel(ConsistencyLevel.Session).build();

    }

    public Map<String,Object> getPaginationResults(int cont, String offSetToken) {


        String query ="select * from c where c.customer.id='11'";

        FeedOptions paginatedFeedOptions = new FeedOptions();
        paginatedFeedOptions.setEnableCrossPartitionQuery(true);
        paginatedFeedOptions.setMaxDegreeOfParallelism(-1);
        paginatedFeedOptions.setMaxItemCount(cont);
        if (StringUtils.isNotBlank(offSetToken)) {
            paginatedFeedOptions.setRequestContinuation(offSetToken);
        }

        Observable<FeedResponse<Document>> queryObservable = documentClient.queryDocuments(getCollectionLink(), query, paginatedFeedOptions);

        TestSubscriber<FeedResponse<Document>> subscriber = new TestSubscriber();
        queryObservable.first().subscribe(subscriber);
        subscriber.awaitTerminalEvent();

        List<FeedResponse<Document>> feedResponseList = subscriber.getOnNextEvents();
        Map<String,Object> resultMap=new HashMap();
        if(CollectionUtils.isNotEmpty(feedResponseList)){
            FeedResponse<Document> page = subscriber.getOnNextEvents().get(0);
            String responseContinuation = page.getResponseContinuation();
            List<String> documents = page.getResults().stream().map(doc -> doc.toJson()).collect(Collectors.toList());
            resultMap.put("token",responseContinuation);
            resultMap.put("documents",documents);

        }
        return resultMap;
    }

    private String getCollectionLink() {
        return String.format("dbs/%s/colls/%s", databaseId, collectionId);
    }
}
