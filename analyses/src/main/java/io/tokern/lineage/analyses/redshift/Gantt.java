package io.tokern.lineage.analyses.redshift;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

class Gantt {
  private static Logger logger = LoggerFactory.getLogger(Gantt.class);
  private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("Y-MM-dd HH:mm:ss");


  static class Entry {
    public final String target;
    public final String startTime;
    public final String endTime;

    Entry(String target, LocalDateTime startTime, LocalDateTime endTime) {
      this.target = target;
      this.startTime = startTime.format(formatter);
      this.endTime = endTime.format(formatter);
    }
  }

  static class TimeSlice {
    public final String time;
    public final long numQueries;
    public final long numInserts;
    public final long numCtas;
    public final long numSelectInto;
    public final long numUnloads;
    public final long numCopy;

    public TimeSlice(LocalDateTime time, long numQueries, long numInserts,
                     long numSelectInto, long numCtas, long numUnloads, long numCopy) {
      this.time = time.format(formatter);
      this.numQueries = numQueries;
      this.numInserts = numInserts;
      this.numCtas = numCtas;
      this.numSelectInto = numSelectInto;
      this.numUnloads = numUnloads;
      this.numCopy = numCopy;
    }
  }

  static List<Entry> sort(List<QueryInfo> queries) {
    List<Entry> entries = new ArrayList<>();

    queries.sort(Comparator.naturalOrder());
    queries.forEach((query) -> {
      String target = null;
      assert (query.classes.insertContext.isPassed()
          || query.classes.ctasContext.isPassed()
          || query.classes.unloadContext.isPassed()
          || query.classes.copyContext.isPassed()
          || query.classes.selectIntoContext.isPassed());

      if (query.classes.insertContext.isPassed()) {
        target = query.classes.insertContext.getTargetTable();
      } else if (query.classes.ctasContext.isPassed()) {
        target = query.classes.ctasContext.getTargetTable();
      } else if (query.classes.selectIntoContext.isPassed()) {
        target = query.classes.selectIntoContext.getTargetTable();
      } else if (query.classes.copyContext.isPassed()) {
        target = query.classes.copyContext.getTargetTable();
      } else if (query.classes.unloadContext.isPassed()) {
        target = "S3";
      }
      entries.add(new Entry(
          target,
          query.query.startTime,
          query.query.endTime
      ));
    });

    return entries;
  }

  static List<TimeSlice> histogram(List<QueryInfo> queries) {
    List<TimeSlice> timeSlices = new ArrayList<>();

    queries.sort(Comparator.naturalOrder());
    LocalDateTime currentTime = queries.get(0).query.startTime.truncatedTo(ChronoUnit.MINUTES);
    LocalDateTime endTime =
        queries.get(queries.size() - 1).query.endTime.truncatedTo(ChronoUnit.MINUTES)
            .plusMinutes(1);

    while (currentTime.isBefore(endTime)) {
      long numQueries = 0;
      long numInserts = 0;
      long numCtas = 0;
      long numSelectInto = 0;
      long numUnloads = 0;
      long numCopy = 0;

      for (QueryInfo query : queries) {
        if ((currentTime.isEqual(query.query.startTime)
            || currentTime.isAfter(query.query.startTime))
            && (currentTime.isBefore(query.query.endTime)
            || currentTime.isEqual(query.query.endTime))) {
          numQueries++;
          if (query.classes.insertContext.isPassed()) {
            numInserts++;
          } else if (query.classes.ctasContext.isPassed()) {
            numCtas++;
          } else if (query.classes.selectIntoContext.isPassed()) {
            numSelectInto++;
          } else if (query.classes.unloadContext.isPassed()) {
            numUnloads++;
          } else if (query.classes.copyContext.isPassed()) {
            numCopy++;
          }
        }

        if (currentTime.isBefore(query.query.startTime)) {
          break;
        }
      }

      timeSlices.add(new TimeSlice(currentTime, numQueries, numInserts, numCtas,
          numSelectInto, numUnloads, numCopy));
      currentTime = currentTime.plusSeconds(15);
    }

    return timeSlices;
  }
}
