package search;

import cluster.management.ServiceRegistry;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.google.protobuf.InvalidProtocolBufferException;
import model.frontend.FrontendSearchRequest;
import model.frontend.FrontendSearchResponse;
import model.proto.SearchModel;
import networking.OnRequestCallback;
import networking.WebClient;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UserSearchHandler implements OnRequestCallback {
    private static final String ENDPOINT = "/documents_search";
    private static final String DOCUMENTS_LOCATION = "books";
    private final ObjectMapper objectMapper;
    private final WebClient client;
    private final ServiceRegistry searchCoordinatorRegistry;

    public UserSearchHandler(ServiceRegistry searchCoordinatorRegistry) {
        this.searchCoordinatorRegistry = searchCoordinatorRegistry;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.objectMapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
        this.client = new WebClient();

    }
    @Override
    public byte[] handleRequest(byte[] requestPayload) {
        try {
            FrontendSearchRequest frontendSearchRequest =
                    objectMapper.readValue(requestPayload, FrontendSearchRequest.class);

            FrontendSearchResponse frontendSearchResponse = createFrontEndResponse(frontendSearchRequest);

            return objectMapper.writeValueAsString(frontendSearchResponse).getBytes();
        } catch (IOException e) {
            e.printStackTrace();
            return new byte []{};
        }
    }

    private FrontendSearchResponse createFrontEndResponse(FrontendSearchRequest frontendSearchRequest) {
        SearchModel.Response searchClusterResponse = sendRequestToSearchCluster(frontendSearchRequest.getSearchQuery());

        List<FrontendSearchResponse.SearchResultInfo> filteredResults =
                filterResults(searchClusterResponse,
                        frontendSearchRequest.getMaxNumberOfResults(),
                        frontendSearchRequest.getMinScore());

        return new FrontendSearchResponse(filteredResults, DOCUMENTS_LOCATION);
    }

    private List<FrontendSearchResponse.SearchResultInfo> filterResults(
            SearchModel.Response searchClusterResponse, long maxResults, double minScore) {
        double maxScore = getMaxScore(searchClusterResponse);
        List<FrontendSearchResponse.SearchResultInfo> searchResultInfoList = new ArrayList<>();
        for (int i = 0; i < searchClusterResponse.getRelevantDocumentsCount() && i < maxResults ; i++) {

            int score = (int) (searchClusterResponse.getRelevantDocuments(i).getScore() * 100.0 /maxScore );
            if (score < minScore) {
                break;
            }

            String title = searchClusterResponse.getRelevantDocuments(i).getDocumentName().split("\\.")[0];
            String extension = searchClusterResponse.getRelevantDocuments(i).getDocumentName().split("\\.")[1];

            FrontendSearchResponse.SearchResultInfo resultInfo =
                    new FrontendSearchResponse.SearchResultInfo(title, extension, score);

            searchResultInfoList.add(resultInfo);
        }

        return searchResultInfoList;
    }

    private static double getMaxScore(SearchModel.Response searchClusterResponse) {
        if ( searchClusterResponse.getRelevantDocumentsCount() == 0) {
            return 0;
        }
        return searchClusterResponse.getRelevantDocumentsList()
                .stream()
                .map(document -> document.getScore())
                .max(Double::compareTo)
                .get();
    }

    @Override
    public String getEndpoint() {
        return ENDPOINT;
    }

    private SearchModel.Response sendRequestToSearchCluster(String searchQuery) {
        SearchModel.Request searchRequest = SearchModel.Request.newBuilder()
                .setSearchQuery(searchQuery)
                .build();

        try {
            String coordinatorAddress = searchCoordinatorRegistry.getRandomServiceAddress();
            if (coordinatorAddress == null) {
                System.out.println("Search Cluster Coordinator is unavailable");
                return SearchModel.Response.getDefaultInstance();
            }

            byte[] payloadBody = client.sendTask(coordinatorAddress, searchRequest.toByteArray()).join();

            return SearchModel.Response.parseFrom(payloadBody);
        } catch (InterruptedException | KeeperException |  InvalidProtocolBufferException e) {
            e.printStackTrace();
            return SearchModel.Response.getDefaultInstance();
        }
    }
}
