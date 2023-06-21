package edu.brunobudris.sdcc.pagerank.model;

import lombok.*;
import lombok.experimental.FieldDefaults;

import java.math.BigDecimal;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@Builder
@FieldDefaults(level = AccessLevel.PRIVATE)
public class PageRank {
    Long vertexId;
    BigDecimal value;
}
