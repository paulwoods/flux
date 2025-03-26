package com.mrpaulwoods.flux.server;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.OffsetDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.Thread.sleep;

@SpringBootApplication
public class ServerApplication {
    public static void main(String[] args) {
        SpringApplication.run(ServerApplication.class, args);
    }
}

record Response(
        int index,
        String message,
        String timestamp
) {
}

@RestController
@RequestMapping("/stream")
@CrossOrigin(origins = "*")
class ServerController implements CommandLineRunner {

    private final Sinks.Many<Response> sink;
    private final Flux<Response> flux;

    public ServerController() {
        this.sink = Sinks.many().multicast().onBackpressureBuffer();
        this.flux = sink.asFlux();
    }

    @GetMapping(value = "/data", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Response> streamData() {
        return flux;
    }

    @Override
    public void run(String... args) {

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {

            executor.submit(() -> {

                try {
                    for (int n = 0; n < 1000; ++n) {

                        Response response = new Response(
                                n,
                                "message %d".formatted(n),
                                OffsetDateTime.now().toString()
                        );

                        System.out.println(response);
//                        Sinks.EmitResult er = sink.tryEmitNext(response);
                        sink.tryEmitNext(response);

                        sleep(1000);
                    }

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

            });

        }

    }

}
