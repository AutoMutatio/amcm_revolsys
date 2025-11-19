package com.revolsys.swing.table.counts;

import java.io.Writer;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.swing.JTable;

import com.revolsys.date.Dates.Timer;
import com.revolsys.parallel.ReentrantLockEx;
import com.revolsys.record.io.format.tsv.Tsv;
import com.revolsys.record.io.format.tsv.TsvWriter;
import com.revolsys.swing.parallel.Invoke;
import com.revolsys.swing.table.AbstractTableModel;
import com.revolsys.swing.table.BaseJTable;

public class TimerTableModel extends AbstractTableModel {
  private static final long serialVersionUID = 1L;

  private static final String[] COLUMN_NAMES = {
    "Label", "Duration"
  };

  private final Map<String, Timer> timerByLabel = new ConcurrentHashMap<>();

  private final List<String> labels = new ArrayList<>();

  private final ReentrantLockEx labelsLock = new ReentrantLockEx();

  public Timer addTimer(final String label) {
    return this.timerByLabel.computeIfAbsent(label, newLabel -> {
      try (
        var _ = labelsLock.lockX()) {
        if (!labels.contains(newLabel)) {
          this.labels.add(newLabel);
        }
      }
      final var timer = new Timer() {
        @Override
        public void close() {
          super.close();
          Invoke.later(TimerTableModel.this::fireTableDataChanged);
        }
      };
      Invoke.later(this::fireTableDataChanged);
      return timer;
    });
  }

  public void clear() {
    this.timerByLabel.clear();
  }

  @Override
  public Class<?> getColumnClass(final int columnIndex) {
    return switch (columnIndex) {
      case 0 -> String.class;
      case 1 -> Duration.class;
      default -> String.class;
    };
  }

  @Override
  public int getColumnCount() {
    return 2;
  }

  @Override
  public String getColumnName(final int columnIndex) {
    return COLUMN_NAMES[columnIndex];
  }

  @Override
  public int getRowCount() {
    return this.timerByLabel.size();
  }

  @Override
  public Object getValueAt(final int rowIndex, final int columnIndex) {
    if (rowIndex >= 0 && rowIndex < this.labels.size()) {
      final var label = this.labels.get(rowIndex);
      return switch (columnIndex) {
        case 0 -> label;
        case 1 -> this.timerByLabel.get(label)
          .duration()
          .truncatedTo(ChronoUnit.SECONDS)
          .toString()
          .substring(1);
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
    return table;
  }

  @Override
  public void toTsv(final Writer out) {
    try (
      TsvWriter tsv = Tsv.plainWriter(out)) {
      tsv.write((Object[])COLUMN_NAMES);
      this.timerByLabel.forEach((label, timer) -> {
        tsv.write(label, timer);
      });
    }
  }

}
