package model.frontend;

import java.util.Collections;
import java.util.List;

public class FrontendSearchResponse {
    private List<SearchResultInfo> searchResults = Collections.emptyList();
    private String documentsLocation = "";

    public static class SearchResultInfo {
        private String title;
        private String extension;
        private int score;
    }
}
