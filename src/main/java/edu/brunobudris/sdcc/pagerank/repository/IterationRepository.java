package edu.brunobudris.sdcc.pagerank.repository;

import lombok.RequiredArgsConstructor;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
@RequiredArgsConstructor
public class IterationRepository {

    private static final String QUERY_COUNT = "SELECT COUNT(1) FROM iteration";

    private static final String QUERY_INSERT = "INSERT INTO iteration"
            + " (iteration, ready, mapped, reduced)"
            + " VALUES (:iteration, false, false, false)";

    private static final String QUERY_SET_TO_READY = "UPDATE iteration SET ready = true"
            + " WHERE iteration = :iteration"
            + " AND ready = false";

    private static final String QUERY_SET_TO_MAPPED = "UPDATE iteration SET mapped = true"
            + " WHERE iteration = :iteration"
            + " AND mapped = false";

    private static final String QUERY_SET_TO_REDUCED = "UPDATE iteration SET reduced = true"
            + " WHERE iteration = :iteration"
            + " AND reduced = false";

    private static final String QUERY_SELECT_READY = "SELECT iteration"
            + " FROM iteration"
            + " WHERE ready = true"
            + " AND mapped = false"
            + " AND reduced = false"
            + " ORDER BY iteration"
            + " FETCH NEXT 1 ROWS ONLY";

    private static final String QUERY_SELECT_MAPPED = "SELECT iteration"
            + " FROM iteration"
            + " WHERE ready = true"
            + " AND mapped = true"
            + " AND reduced = false"
            + " ORDER BY iteration"
            + " FETCH NEXT 1 ROWS ONLY";

    private static final String QUERY_COUNT_NOT_MAPPED = "SELECT COUNT(1)"
            + " FROM iteration"
            + " WHERE mapped = false";

    private static final String QUERY_COUNT_NOT_REDUCED = "SELECT COUNT(1)"
            + " FROM iteration"
            + " WHERE reduced = false";

    private static final String QUERY_SELECT_IS_MAPPED = "SELECT mapped"
            + " FROM iteration"
            + " WHERE iteration = :iteration";

    private static final String QUERY_SELECT_IS_REDUCED = "SELECT reduced"
            + " FROM iteration"
            + " WHERE iteration = :iteration";

    private static final String ITERATION_PARAM = "iteration";


    private final NamedParameterJdbcTemplate jdbcTemplate;

    /**
     * Counts the total number of records in the "iteration" table
     * @return zero or positive integer
     */
    public int countIteration() {
        try {
            final var count = jdbcTemplate.queryForObject(QUERY_COUNT, new MapSqlParameterSource(), Integer.class);
            return count == null ? 0 : count;
        } catch (EmptyResultDataAccessException exception) {
            return 0;
        }
    }

    /**
     * Batch insert into the "iteration" table
     * @param iterations the total number of iterations to be inserted
     * @return the total number of inserted records, should be same as iterations
     */
    public int[] insert(int iterations) {
        final var params = new MapSqlParameterSource[iterations];

        for (int iteration = 0; iteration < iterations; iteration++) {
            params[iteration] = new MapSqlParameterSource()
                    .addValue(ITERATION_PARAM, iteration + 1);
        }

        return jdbcTemplate.batchUpdate(QUERY_INSERT, params);
    }

    /**
     * Mark an iteration as 'ready' (to be mapped)
     * @param iteration iteration number
     * @return 1 if the iteration was successfully marked as ready (if it has not already been marked as ready),
     * 0 otherwise.
     */
    public int markIterationAsReady(Integer iteration) {
        return jdbcTemplate.update(QUERY_SET_TO_READY, new MapSqlParameterSource(ITERATION_PARAM, iteration));
    }

    /**
     * Mark an iteration as 'mapped'
     * @param iteration iteration number
     * @return 1 if the iteration was successfully marked as mapped (if it has not already been marked as mapped),
     * 0 otherwise.
     */
    public int markIterationAsMapped(Integer iteration) {
        return jdbcTemplate.update(QUERY_SET_TO_MAPPED, new MapSqlParameterSource(ITERATION_PARAM, iteration));
    }

    /**
     * Mark an iteration as 'reduced'
     * @param iteration iteration number
     * @return 1 if the iteration was successfully marked as reduced (if it has not already been marked as reduced),
     * 0 otherwise.
     */
    public int markIterationAsReduced(Integer iteration) {
        return jdbcTemplate.update(QUERY_SET_TO_REDUCED, new MapSqlParameterSource(ITERATION_PARAM, iteration));
    }

    /**
     * Get an iteration that is ready to be mapped. If there are several such iterations, the one with the
     * lowest iteration number is selected
     * @return an iteration if there is any
     */
    public Optional<Integer> getReady() {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(QUERY_SELECT_READY,
                    new MapSqlParameterSource(), getIterationRowMapper()));
        } catch (EmptyResultDataAccessException exception) {
            return Optional.empty();
        }
    }

    /**
     * Get an iteration that is mapped and ready to be reduced. If there are several such iterations, the one with the
     * lowest iteration number is selected
     * @return an iteration if there is any
     */
    public Optional<Integer> getMapped() {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(QUERY_SELECT_MAPPED,
                    new MapSqlParameterSource(), getIterationRowMapper()));
        } catch (EmptyResultDataAccessException exception) {
            return Optional.empty();
        }
    }

    /**
     * Checks if an iteration is mapped
     * @param iteration iteration number
     * @return true if iteration is marked as mapped, false otherwise
     */
    public boolean isMapped(Integer iteration) {
        final var result = jdbcTemplate.queryForObject(QUERY_SELECT_IS_MAPPED,
                new MapSqlParameterSource(ITERATION_PARAM, iteration),
                Boolean.class);

        if (result == null) {
            throw new IllegalArgumentException("Nullable boolean result");
        }

        return result;
    }

    /**
     * Checks if an iteration is reduced
     * @param iteration iteration number
     * @return true if iteration is marked as reduced, false otherwise
     */
    public boolean isReduced(Integer iteration) {
        final var result = jdbcTemplate.queryForObject(QUERY_SELECT_IS_REDUCED,
                new MapSqlParameterSource(ITERATION_PARAM, iteration),
                Boolean.class);

        if (result == null) {
            throw new IllegalArgumentException("Nullable boolean result");
        }

        return result;
    }

    /**
     * Checks if all iterations are mapped
     * @return true if there is no iteration marked as not mapped, false otherwise
     */
    public boolean isAllMapped() {
        try {
            final var count = jdbcTemplate.queryForObject(QUERY_COUNT_NOT_MAPPED,
                    new MapSqlParameterSource(), Integer.class);
            return count == null || count < 1;
        } catch (EmptyResultDataAccessException exception) {
            return true;
        }
    }

    /**
     * Checks if all iterations are reduced
     * @return true if there is no iteration marked as not reduced, false otherwise
     */
    public boolean isAllReduced() {
        try {
            final var count = jdbcTemplate.queryForObject(QUERY_COUNT_NOT_REDUCED,
                    new MapSqlParameterSource(), Integer.class);
            return count == null || count < 1;
        } catch (EmptyResultDataAccessException exception) {
            return true;
        }
    }

    private RowMapper<Integer> getIterationRowMapper() {
        return (rs,row) -> rs.getInt(ITERATION_PARAM);
    }
}