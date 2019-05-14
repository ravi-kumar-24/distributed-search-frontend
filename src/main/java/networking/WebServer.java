package networking;

import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.concurrent.Executors;

public class WebServer {
    private static final String STATUS_ENDPOINT = "/status";
    private static final String HOME_PAGE_ENDPOING = "/";

    private static final String HOME_PAGE_ASSETS_BASE_DIR = "/ui_assets/";


    private final int port;
    private HttpServer server;
    private final OnRequestCallback requestCallback;

    public WebServer(int port, OnRequestCallback requestCallback) {
        this.port = port;
        this.requestCallback = requestCallback;
    }

    public void startServer() {
        try {
            this.server = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        HttpContext statusContext = server.createContext(STATUS_ENDPOINT);
        HttpContext taskContext = server.createContext(requestCallback.getEndpoint());

        statusContext.setHandler(this::handleStatusCheckRequest);
        taskContext.setHandler(this::handleTaskRequest);

        // handle html page load
        HttpContext homePageContext = server.createContext(HOME_PAGE_ENDPOING);
        homePageContext.setHandler(this::handleHomePageRequest);

        server.setExecutor(Executors.newFixedThreadPool(8));
        server.start();
    }

    private void handleHomePageRequest(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase("get")) {
            exchange.close();
            return;
        }

        byte [] response;

        String asset = exchange.getRequestURI().getPath();

        if (asset.equals(HOME_PAGE_ENDPOING)) {
            response = readUiAsset(HOME_PAGE_ASSETS_BASE_DIR + "index.html");
        } else {
            response = readUiAsset(asset);
        }
        addContentType(asset, exchange);

        sendResponse(response, exchange);
    }

    private byte [] readUiAsset(String asset) throws IOException {
        URL assetUrl = getClass().getResource( asset);
        if ( assetUrl == null) {
            return new byte[]{};
        }

        URI assetFile  = URI.create(assetUrl.getFile());
        FileInputStream fileInputStream = new FileInputStream(assetFile.getPath());
        return fileInputStream.readAllBytes();
    }

    private static void addContentType(String asset, HttpExchange exchange) {
        String contentType = "text/html";
        if (asset.endsWith("js")) {
            contentType = "text/javascript";
        } else if (asset.endsWith("css")) {
            contentType = "text/css";
        }
        exchange.getResponseHeaders().add("Content-Type", contentType);
    }

    private void handleTaskRequest(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase("post")) {
            exchange.close();
            return;
        }

        byte [] responseBytes = requestCallback.handleRequest(exchange.getRequestBody().readAllBytes());

        sendResponse(responseBytes, exchange);
    }

    private void handleStatusCheckRequest(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase("get")) {
            exchange.close();
            return;
        }

        String responseMessage = "Server is alive\n";
        sendResponse(responseMessage.getBytes(), exchange);
    }

    private void sendResponse(byte [] responseBytes, HttpExchange exchange) throws IOException {
        exchange.sendResponseHeaders(200, responseBytes.length);
        OutputStream outputStream = exchange.getResponseBody();
        outputStream.write(responseBytes);
        outputStream.flush();
        outputStream.close();
    }
}
