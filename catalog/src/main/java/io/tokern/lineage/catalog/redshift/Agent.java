package io.tokern.lineage.catalog.redshift;

import io.tokern.lineage.catalog.util.MetricAgentException;

import java.time.LocalDateTime;
import java.util.List;

public interface Agent {
  List<UserQuery> getQueries(LocalDateTime rangeStart, LocalDateTime rangeEnd)
      throws MetricAgentException;
}
