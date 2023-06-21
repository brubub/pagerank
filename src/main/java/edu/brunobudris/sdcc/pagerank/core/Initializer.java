package edu.brunobudris.sdcc.pagerank.core;

import edu.brunobudris.sdcc.pagerank.repository.IterationRepository;
import edu.brunobudris.sdcc.pagerank.repository.PageRankRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Component
@RequiredArgsConstructor
@Slf4j
public class Initializer {

    private final IterationRepository iterationRepository;
    private final PageRankRepository pageRankRepository;

    @Value("${graph.iteration-limit}")
    private Integer iterationLimit;

    /**
     * Database population
     * @param graph the graph to be ranked
     */
    public void init(Graph<Long, DefaultEdge> graph) {
        var count = iterationRepository.countIteration();

        if (count < 1) {
            // the "iteration" table is empty
            final var inserted = iterationRepository.insert(iterationLimit);

            if (inserted.length != iterationLimit) {
                log.info("Mismatch between inserted iteration rows and statements");
            }
        } else if (iterationRepository.isAllReduced()) {
            // the previous ranking is fully completed
            throw new RuntimeException("All iterations completed, no work left (drop table)");
        }

        count = pageRankRepository.countRanks();
        if (count < 1) {
            // the "page_rank" table is empty
            final var vertexSet = graph.vertexSet();
            final var graphCardinality = vertexSet.size();
            // scale is equal to the db column datatype scale
            final var value = BigDecimal.ONE.setScale(30).divide(new BigDecimal(graphCardinality), RoundingMode.DOWN);

            if (value.equals(BigDecimal.ZERO)) {
                // db column datatype scale must be increased
                log.error("Initial page rank is zero, graph cardinality: {}", graphCardinality);
                throw new IllegalArgumentException("Initial value is zero");
            }

            try {
                final var inserted = pageRankRepository.insert(vertexSet, value);

                if (inserted.length != graphCardinality) {
                    throw new RuntimeException("Mismatch between inserted ranks and statements");
                }
            } catch (DuplicateKeyException exception) {
                // iterations were inserted by application another node
            }
        }

        // we mark the first PageRank algorithm iteration as ready to be processed (idempotent)
        iterationRepository.markIterationAsReady(1);
    }
}
