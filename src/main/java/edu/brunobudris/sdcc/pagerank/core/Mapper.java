package edu.brunobudris.sdcc.pagerank.core;

import edu.brunobudris.sdcc.pagerank.model.PageRank;
import edu.brunobudris.sdcc.pagerank.repository.IterationRepository;
import edu.brunobudris.sdcc.pagerank.repository.MapVoteRepository;
import edu.brunobudris.sdcc.pagerank.repository.PageRankRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
@Slf4j
public class Mapper {

    private final PageRankRepository pageRankRepository;
    private final IterationRepository iterationRepository;
    private final MapVoteRepository mapVoteRepository;

    @Async
    public CompletableFuture<Integer> execute(Graph<Long, DefaultEdge> graph) {
        log.info("A mapper is ready");
        try {
            while (!iterationRepository.isAllMapped()) {
                // the iteration that is ready to be mapped
                final var optional = iterationRepository.getReady();

                if (optional.isEmpty()) {
                    // no mapped iteration, waiting for reducers to finish their work
                    log.info("A mapper is waiting for an iteration to be ready");
                    Thread.sleep(1000);
                    continue;
                }

                // mapping outbound values
                map(graph, optional.get());
            }
            // all iterations mapped - PageRank algorithm map phase terminated
            log.info("A mapper finished normally");
            return CompletableFuture.completedFuture(1);
        } catch (Exception exception) {
            log.error("A mapper finished with error", exception);
            return CompletableFuture.failedFuture(exception);
        }
    }

    private void map(Graph<Long, DefaultEdge> graph, Integer iteration) {
        log.info("Start of mapping iteration # {}", iteration);
        // Web page ranking in previous iteration
        final var ranks = pageRankRepository.getPagesRank(iteration - 1);

        // Page randomization allows us to evenly distribute the workload between mappers
        Collections.shuffle(ranks);

        // vertices that are already mapped
        List<Long> alreadyMapped = mapVoteRepository.getSourcesId(iteration);
        // No more than once per second we check if this iteration has been mapped
        var nextCheckTime = LocalDateTime.now().plusSeconds(1);

        for (final var pageRank : ranks) {
            if (LocalDateTime.now().isAfter(nextCheckTime)) {
                if (iterationRepository.isMapped(iteration)) {
                    // another mapper finished this iteration mapping phase before us
                    log.info("iteration # {} was mapped by another worker", iteration);
                    return;
                } else {
                    alreadyMapped = mapVoteRepository.getSourcesId(iteration);
                    // next check time
                    nextCheckTime = LocalDateTime.now().plusSeconds(1);
                }
            }

            if (alreadyMapped.contains(pageRank.getVertexId())) {
                continue;
            }

            mapVertex(graph, pageRank, iteration);
        }

        final var updated = iterationRepository.markIterationAsMapped(iteration);
        if (updated > 0) {
            log.info("Iteration # {} was marked as mapped", iteration);
        }
        log.info("End of mapping iteration # {}", iteration);
    }

    private void mapVertex(Graph<Long, DefaultEdge> graph, PageRank pageRank, Integer iteration) {
        final var outgoingEdges = graph.outgoingEdgesOf(pageRank.getVertexId());
        final var size = outgoingEdges.size();
        if (size == 0) {
            // vertex has no outgoing edges - it is a sink
            return;
        }

        // outbound value - map vote
        final var vote = pageRank.getValue().divide(new BigDecimal(size), RoundingMode.DOWN);

        // vertex is connected to these vertices
        final var neighbours = outgoingEdges.stream()
                .map(graph::getEdgeTarget)
                .toList();

        try {
            mapVoteRepository.insert(pageRank.getVertexId(), neighbours, iteration, vote);
        } catch (DuplicateKeyException exception) {
            // this vertex was already mapped by another mapper
        }
    }
}
