package company.vk.edu.distrib.compute.andeco.replica;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;

@SuppressWarnings("PMD.ImplicitFunctionalInterface")
public interface Controller {
    void processRequest(HttpExchange exchange) throws IOException;
}
