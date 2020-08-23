package elasticsearch;

import no.unit.nva.dynamodb.PublicationIndexDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class ElasticSearchRestClient {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchRestClient.class);

    public ElasticSearchRestClient() {
//        RestClient esClient = esClient(serviceName, region);
    }


    public void addDocumentToIndex(PublicationIndexDocument document) {
        logger.debug("Upserting search index  with values {}", document);
    }

    public void removeDocumentFromIndex(String identifier) {
        logger.trace("Deleting from search API publication with identifier: {}", identifier);


    }


}
