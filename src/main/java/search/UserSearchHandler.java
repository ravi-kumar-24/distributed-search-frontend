package search;

import cluster.management.ServiceRegistry;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.google.protobuf.InvalidProtocolBufferException;
import model.proto.SearchModel;
import networking.OnRequestCallback;
import networking.WebClient;
import org.apache.zookeeper.KeeperException;

public class UserSearchHandler implements OnRequestCallback {
    private static final String ENDPOINT = "/documents_search";
    private final ObjectMapper objectMapper;
    private final WebClient client;
    private final ServiceRegistry searchCoordinatorRegistry;

    public UserSearchHandler(ServiceRegistry searchCoordinatorRegistry) {
        this.searchCoordinatorRegistry = searchCoordinatorRegistry;
        this.client = new WebClient();
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.objectMapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
    }


    @Override
    public byte[] handleRequest(byte[] requestPayload) {
        return new byte[0];
    }

    @Override
    public String getEndpoint() {
        return ENDPOINT;
    }

    private static String getDocumentExtension(String document) {
        String[] parts = document.split("\\.");
        if (parts.length == 2) {
            return parts[1];
        }
        return "";
    }

    private static String getDocumentTitle(String document) {
        return document.split("\\.")[0];
    }

    private static int normalizeScore(double inputScore, double maxScore) {
        return (int) (inputScore * 100.0 / maxScore);
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
