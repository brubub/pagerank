package edu.brunobudris.sdcc.pagerank.core;

import edu.brunobudris.sdcc.pagerank.io.DotGraphBuilder;
import edu.brunobudris.sdcc.pagerank.io.HttpGraphProvider;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
@RequiredArgsConstructor
@Slf4j
public class Runner {

    private final ApplicationContext applicationContext;
    private final HttpGraphProvider graphProvider;
    private final Initializer initializer;
    private final Mapper mapper;
    private final Reducer reducer;

    @Value("${graph.mapper}")
    private boolean isMapper;

    @Value("${graph.reducer}")
    private boolean isReducer;

    @EventListener(ApplicationReadyEvent.class)
    public void runApplication() {
        try {
            run();
            shutdown(null);
        } catch (Exception exception) {
            shutdown(exception);
        }
    }

    private void run() throws Exception {
        final var data = graphProvider.provide();
        final var graph = DotGraphBuilder.build(data);
        initializer.init(graph);

        CompletableFuture<Integer> mapperFuture = null;
        CompletableFuture<Integer> reducerFuture = null;

        if (isMapper) {
            // async task
            mapperFuture = mapper.execute(graph);
        }

        if (isReducer) {
            // async task
            reducerFuture = reducer.execute(graph);
        }

        if (mapperFuture != null && reducerFuture == null) {
            mapperFuture.get();
        } else if (mapperFuture == null && reducerFuture != null) {
            reducerFuture.get();
        } else if (mapperFuture != null) {
            final var anyOf = CompletableFuture.anyOf(mapperFuture, reducerFuture);

            try {
                anyOf.get();
            } catch (Exception exception) {
                // when mapper or reducer is completed exceptionally, we also cancel the other one too.
                mapperFuture.cancel(false);
                reducerFuture.cancel(false);
                throw exception;
            }

            if (mapperFuture.isDone()) {
                // when mapper completes normally, we wait for reducer to complete.
                reducerFuture.get();
            } else {
                // the reducer cannot complete before the mapper, so we cancel the mapper.
                mapperFuture.cancel(false);
                throw new RuntimeException("The reducer completed before the mapper");
            }
        } else {
            throw new RuntimeException("Neither the mapper nor the reducer has been started.");
        }
    }

    private void shutdown(Exception exception) {
        if (exception != null) {
            log.error("Unexpected error", exception);
        } else {
            log.info("Application exited normally");
        }

        SpringApplication.exit(applicationContext, () -> {
            if (exception == null) {
                return 0;
            } else {
                return -1;
            }
        });
    }

}
