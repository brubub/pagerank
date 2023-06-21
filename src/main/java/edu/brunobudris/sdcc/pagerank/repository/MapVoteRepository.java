package edu.brunobudris.sdcc.pagerank.repository;

import edu.brunobudris.sdcc.pagerank.model.MapVote;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class MapVoteRepository {

    private static final String QUERY_SELECT_MAP_VALUES = "SELECT target_id, vote"
            + " FROM map_vote"
            + " WHERE iteration = :iteration";

    private static final String QUERY_SELECT_DISTINCT_VERTICES = "SELECT DISTINCT(source_id)"
            + " FROM map_vote"
            + " WHERE iteration = :iteration";

    private static final String QUERY_INSERT_MAP_VALUE = "INSERT INTO map_vote"
            + " (source_id, target_id, iteration, vote)"
            + " VALUES (:source_id, :target_id, :iteration, :vote)";

    private static final String SOURCE_ID_PARAM = "source_id";
    private static final String TARGET_ID_PARAM = "target_id";
    private static final String VOTE_PARAM = "vote";
    private static final String ITERATION_PARAM = "iteration";

    private final NamedParameterJdbcTemplate jdbcTemplate;

    /**
     * Retrieves map votes (i.e. outbound values) for a particular iteration
     * @param iteration iteration number
     * @return list of map votes
     */
    public List<MapVote> getMapValues(Integer iteration) {
        try {
            return jdbcTemplate.query(QUERY_SELECT_MAP_VALUES,
                    new MapSqlParameterSource(ITERATION_PARAM, iteration),
                    getRowMapper());
        } catch (EmptyResultDataAccessException exception) {
            return List.of();
        }
    }

    /**
     * Retrieves the source vertex IDs for a particular iteration (vertices for which outbound values were calculated)
     * @param iteration iteration number
     * @return list of source vertex IDs
     */
    public List<Long> getSourcesId(Integer iteration) {
        try {
            return jdbcTemplate.query(QUERY_SELECT_DISTINCT_VERTICES,
                    new MapSqlParameterSource(ITERATION_PARAM, iteration),
                    (rs,row) -> rs.getLong(SOURCE_ID_PARAM));
        } catch (EmptyResultDataAccessException exception) {
            return List.of();
        }
    }

    /**
     * Batch insert into the "map_vote" table
     * @param sourceId source vertex ID
     * @param targets target vertex IDs
     * @param iteration iteration number
     * @param value map vote (i.e. outbound value)
     */
    public void insert(Long sourceId, List<Long> targets, Integer iteration, BigDecimal value) {
        final var params = targets.stream()
                .map(id -> new MapSqlParameterSource()
                        .addValue(SOURCE_ID_PARAM, sourceId)
                        .addValue(TARGET_ID_PARAM, id)
                        .addValue(ITERATION_PARAM, iteration)
                        .addValue(VOTE_PARAM, value))
                .toArray(SqlParameterSource[]::new);

        jdbcTemplate.batchUpdate(QUERY_INSERT_MAP_VALUE, params);
    }

    private RowMapper<MapVote> getRowMapper() {
        return (rs,row) ->
                MapVote.builder()
                        .targetId(rs.getLong(TARGET_ID_PARAM))
                        .value(rs.getObject(VOTE_PARAM, BigDecimal.class))
                        .build();
    }

}
