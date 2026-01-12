package com.revolsys.swing.table.counts;

import java.io.Writer;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.swing.JTable;

import com.revolsys.date.Dates.Timer;
import com.revolsys.record.io.format.tsv.Tsv;
import com.revolsys.record.io.format.tsv.TsvWriter;
import com.revolsys.swing.parallel.Invoke;
import com.revolsys.swing.table.AbstractTableModel;
import com.revolsys.swing.table.BaseJTable;

public class TimerCounterTableModel extends AbstractTableModel {

  public class TimerCounter extends Timer {
    private final AtomicLong count = new AtomicLong();

    public TimerCounter add() {
      this.count.incrementAndGet();
      return this;
    }

    public TimerCounter add(final long count) {
      this.count.addAndGet(count);
      return this;
    }

    public long count() {
      return this.count.get();
    }
  }

  private static final long serialVersionUID = 1L;

  private final String[] columnNames;

  private final Map<String, TimerCounter> timerByLabel = new ConcurrentHashMap<>();

  private final List<String> labels = new ArrayList<>();

  public TimerCounterTableModel(final String labelColumnTitle) {
    this.columnNames = new String[] {
      labelColumnTitle, "Count", "Duration"
    };
  }

  public TimerCounter addTimer(final String label) {
    return this.timerByLabel.computeIfAbsent(label, newLabel -> {
      this.labels.add(newLabel);
      final var timer = new TimerCounter() {
        @Override
        public void close() {
          super.close();
          Invoke.later(TimerCounterTableModel.this::fireTableDataChanged);
        }
      };
      Invoke.later(this::fireTableDataChanged);
      return timer;
    });
  }

  public void clear() {
    this.labels.clear();
    this.timerByLabel.clear();
  }

  @Override
  public Class<?> getColumnClass(final int columnIndex) {
    return switch (columnIndex) {
      case 0 -> String.class;
      case 1 -> Long.class;
      case 2 -> Duration.class;
      default -> String.class;
    };
  }

  @Override
  public int getColumnCount() {
    return 3;
  }

  @Override
  public String getColumnName(final int columnIndex) {
    return this.columnNames[columnIndex];
  }

  @Override
  public int getRowCount() {
    return this.timerByLabel.size();
  }

  @Override
  public Object getValueAt(final int rowIndex, final int columnIndex) {
    if (rowIndex >= 0 && rowIndex < this.labels.size()) {
      final var label = this.labels.get(rowIndex);
      final var timerCounter = this.timerByLabel.get(label);
      return switch (columnIndex) {
        case 0 -> label;
        case 1 -> timerCounter.count();
        case 2 -> timerCounter.duration().truncatedTo(ChronoUnit.SECONDS);
        default -> null;
      };
    }
    return null;
  }

  @Override
  public BaseJTable newTable() {
    final var table = super.newTable();
    table.setAutoResizeMode(JTable.AUTO_RESIZE_ALL_COLUMNS);
    table.getColumn(1).setMaxWidth(100);
    table.getColumn(2).setMaxWidth(100);
    return table;
  }

  @Override
  public void toTsv(final Writer out) {
    try (
      TsvWriter tsv = Tsv.plainWriter(out)) {
      tsv.write((Object[])this.columnNames);
      this.timerByLabel.forEach((label, timer) -> {
        tsv.write(label, timer);
      });
    }
  }

}
