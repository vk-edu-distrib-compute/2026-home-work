package company.vk.edu.distrib.compute.tadzhnahal;

import company.vk.edu.distrib.compute.KVService;

public class LoadTestServer {
    public static void main(String[] args) throws Exception {
        int port = 8080;

        KVService service = new TadzhnahalKVServiceFactory().create(port);
        service.start();

        System.out.println("Load test server started on port " + port);
        System.out.println("Press Enter to stop...");

        System.in.read();

        service.stop();
        System.out.println("Load test server stopped");
    }
}
