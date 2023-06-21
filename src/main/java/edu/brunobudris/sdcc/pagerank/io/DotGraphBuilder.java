package edu.brunobudris.sdcc.pagerank.io;

import lombok.extern.slf4j.Slf4j;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.nio.dot.DOTImporter;

import java.io.ByteArrayInputStream;

@Slf4j
public class DotGraphBuilder {

    public static Graph<Long, DefaultEdge> build(byte[] data) {
        final var graph = new SimpleDirectedGraph<Long, DefaultEdge>(DefaultEdge.class);
        final var importer = new DOTImporter<Long, DefaultEdge>();

        importer.setVertexFactory(Long::valueOf);

        try (final var bis = new ByteArrayInputStream(data)) {
            importer.importGraph(graph,bis);
            return graph;
        } catch (Exception exception) {
            log.error("Error during dot graph mapping", exception);
            throw new RuntimeException(exception);
        }
    }
}
