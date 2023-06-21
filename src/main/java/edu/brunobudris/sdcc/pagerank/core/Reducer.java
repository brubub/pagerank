package edu.brunobudris.sdcc.pagerank.core;

import edu.brunobudris.sdcc.pagerank.model.MapVote;
import edu.brunobudris.sdcc.pagerank.repository.IterationRepository;
import edu.brunobudris.sdcc.pagerank.repository.MapVoteRepository;
import edu.brunobudris.sdcc.pagerank.repository.PageRankRepository;
import lombok.extern.slf4j.Slf4j;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
public class Reducer {

    private final BigDecimal dampingFactor;
    private final MapVoteRepository mapVoteRepository;
    private final IterationRepository iterationRepository;
    private final PageRankRepository pageRankRepository;

    @Autowired
    public Reducer(@Value("${graph.damping-factor}") Double dampingFactor, MapVoteRepository mapVoteRepository,
                   IterationRepository iterationRepository, PageRankRepository pageRankRepository)  {
        this.dampingFactor = new BigDecimal(dampingFactor);
        this.mapVoteRepository = mapVoteRepository;
        this.iterationRepository = iterationRepository;
        this.pageRankRepository = pageRankRepository;
    }

    @Async
    public CompletableFuture<Integer> execute(Graph<Long, DefaultEdge> graph) {
        try {
            log.info("A reducer is ready");

            final var sinkProbability = BigDecimal.ONE
                    .subtract(this.dampingFactor)
                    .divide(new BigDecimal(graph.vertexSet().size()), RoundingMode.DOWN);

            while (!iterationRepository.isAllReduced()) {
                final var optional = iterationRepository.getMapped();

                if (optional.isEmpty()) {
                    // no available mapped iteration, waiting for mappers to finish their work
                    log.info("A reducer is waiting for an iteration to be mapped");
                    Thread.sleep(1000);
                    continue;
                }

                // summing outbound values (map votes)
                reduce(graph, optional.get(), sinkProbability);
            }
            // all iterations completed - PageRank algorithm terminated
            log.info("A reducer finished");
            return CompletableFuture.completedFuture(1);
        } catch (Exception exception) {
            log.error("A reducer finished with error", exception);
            return CompletableFuture.failedFuture(exception);
        }
    }

    private void reduce(Graph<Long, DefaultEdge> graph, Integer iteration, BigDecimal sinkProbability) {
        log.info("Start of reducing iteration # {}", iteration);
        // web pages
        final var vertices = new LinkedList<>(graph.vertexSet());

        // Page randomization allows us to evenly distribute the workload between mappers
        Collections.shuffle(vertices);

        // map votes (outbound values) calculated during map phase
        final var values = mapVoteRepository.getMapValues(iteration);

        // vertices that are already reduced
        var alreadyReduced = pageRankRepository.getPages(iteration);
        // No more than once per second we check if this iteration has been reduced
        var nextCheckTime = LocalDateTime.now().plusSeconds(1);

        for (final var vertex : vertices) {
            if (LocalDateTime.now().isAfter(nextCheckTime)) {
                if (iterationRepository.isReduced(iteration)) {
                    // another reducer finished this iteration reducing phase before us
                    log.info("iteration # {} was reduced by another worker", iteration);
                    return;
                } else {
                    alreadyReduced = pageRankRepository.getPages(iteration);
                    // next check time
                    nextCheckTime = LocalDateTime.now().plusSeconds(1);
                }
            }

            if (alreadyReduced.contains(vertex)) {
                continue;
            }

            reduceVertex(vertex, sinkProbability, iteration, values);
        }

        var updated = iterationRepository.markIterationAsReduced(iteration);
        if (updated > 0) {
            log.info("Iteration # {} was marked as reduced", iteration);
        }
        updated = iterationRepository.markIterationAsReady(iteration + 1);
        if (updated > 0) {
            log.info("Iteration # {} was marked as ready", iteration + 1);
        }
        log.info("End of reducing iteration # {}", iteration);
    }

    private void reduceVertex(Long vertex, BigDecimal sinkProbability, Integer iteration, List<MapVote> values) {
        var rank = BigDecimal.ZERO;

        for (final var mapValue : values) {
            if (vertex.equals(mapValue.getTargetId())) {
                rank = rank.add(mapValue.getValue());
            }
        }

        rank = rank.multiply(dampingFactor);
        rank = rank.add(sinkProbability);

        try {
            pageRankRepository.insert(vertex, iteration, rank);
        } catch (DuplicateKeyException exception) {
            // this value was already inserted by another reducer
        }
    }
}
