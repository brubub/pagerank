package edu.brunobudris.sdcc.pagerank.repository;

import edu.brunobudris.sdcc.pagerank.model.PageRank;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.List;

@Repository
@RequiredArgsConstructor
@Slf4j
public class PageRankRepository {

    private static final String QUERY_COUNT_RANKS = "SELECT COUNT(1) FROM page_rank WHERE iteration = 1";

    private static final String QUERY_INSERT = "INSERT INTO page_rank"
            + " (vertex_id, iteration, rank)"
            + " VALUES (:vertex_id, :iteration, :rank)";

    private static final String QUERY_SELECT_RANKS = "SELECT vertex_id, rank"
            + " FROM page_rank"
            + " WHERE iteration = :iteration";

    private static final String QUERY_SELECT_DISTINCT_PAGES = "SELECT DISTINCT(vertex_id)"
            + " FROM page_rank"
            + " WHERE iteration = :iteration";

    private static final String ITERATION_PARAM = "iteration";
    private static final String VERTEX_ID_PARAM = "vertex_id";
    private static final String RANK_PARAM = "rank";

    private final NamedParameterJdbcTemplate jdbcTemplate;

    /**
     * Counts the total number of records in the "page_rank" table
     * @return zero or positive integer
     */
    public int countRanks() {
        try {
            final var count = jdbcTemplate.queryForObject(QUERY_COUNT_RANKS, new MapSqlParameterSource(), Integer.class);
            return count == null ? 0 : count;
        } catch (EmptyResultDataAccessException exception) {
            return 0;
        }
    }

    /**
     * Batch insert into the "page_rank" table for iteration #0
     * @param vertices collection of vertices to be inserted
     * @param value value to be assigned to all vertices
     * @return the total number of inserted records, should be same as size of vertices
     */
    public int[] insert(Collection<Long> vertices, BigDecimal value) {
        final var params = prepareRankParams(vertices, value);
        return jdbcTemplate.batchUpdate(QUERY_INSERT, params);
    }

    /**
     * Insert into the "page_rank" table
     * @param vertex graph vertex id
     * @param iteration iteration number
     * @param value vertex rank
     */
    public void insert(Long vertex, Integer iteration, BigDecimal value) {
        jdbcTemplate.update(QUERY_INSERT,
                new MapSqlParameterSource()
                        .addValue(VERTEX_ID_PARAM, vertex)
                        .addValue(ITERATION_PARAM, iteration)
                        .addValue(RANK_PARAM, value));
    }

    /**
     * Retrieves vertex IDs and corresponding ranks for a particular iteration
     * @param iteration iteration number
     * @return list of vertex ID and rank
     */
    public List<PageRank> getPagesRank(Integer iteration) {
        return jdbcTemplate.query(QUERY_SELECT_RANKS,
                new MapSqlParameterSource(ITERATION_PARAM, iteration),
                getRowMapper());
    }

    /**
     * Retrieves vertex IDs for a particular iteration
     * @param iteration iteration number
     * @return list of vertex IDs
     */
    public List<Long> getPages(Integer iteration) {
        return jdbcTemplate.query(QUERY_SELECT_DISTINCT_PAGES,
                new MapSqlParameterSource(ITERATION_PARAM, iteration),
                (rs,row) -> rs.getLong(VERTEX_ID_PARAM));
    }

    private RowMapper<PageRank> getRowMapper() {
        return (rs,row) ->
                PageRank.builder()
                        .vertexId(rs.getLong(VERTEX_ID_PARAM))
                        .value(rs.getObject(RANK_PARAM, BigDecimal.class))
                        .build();
    }

    private MapSqlParameterSource[] prepareRankParams(Collection<Long> vertices, BigDecimal value) {
        return vertices.stream()
                .map(v -> new MapSqlParameterSource()
                        .addValue(VERTEX_ID_PARAM, v)
                        .addValue(ITERATION_PARAM, 0)
                        .addValue(RANK_PARAM, value))
                .toArray(MapSqlParameterSource[]::new);
    }
}
