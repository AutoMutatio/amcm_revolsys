package com.revolsys.swing.table.counts;

import java.awt.Component;
import java.io.Writer;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.swing.JTable;
import javax.swing.RowSorter.SortKey;
import javax.swing.SortOrder;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.TableColumn;

import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;
import com.revolsys.record.io.format.tsv.Tsv;
import com.revolsys.record.io.format.tsv.TsvWriter;
import com.revolsys.swing.Icons;
import com.revolsys.swing.parallel.Invoke;
import com.revolsys.swing.table.AbstractTableModel;
import com.revolsys.swing.table.BaseJTable;
import com.revolsys.util.BaseCloseable;
import com.revolsys.util.count.LabelCountMap;

public class TimerLabelCountTableModel extends AbstractTableModel {

  public class TimerLabelCountMap extends LabelCountMap implements BaseCloseable {
    private final Instant startTime = Instant.now();

    private Instant endTime = null;

    @Override
    public void close() {
      this.endTime = Instant.now();
    }

    public Duration duration() {
      Instant end;
      if (this.endTime == null) {
        end = Instant.now();
      } else {
        end = this.endTime;
      }
      return Duration.between(this.startTime, end);
    }

    public boolean isClosed() {
      return endTime != null;
    }
  }

  private static final long serialVersionUID = 1L;

  public static final DefaultTableCellRenderer BOOLEAN_RENDERER = new DefaultTableCellRenderer() {
    @Override
    public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected,
      boolean hasFocus, int row, int column) {
      final Component component = super.getTableCellRendererComponent(table, value, isSelected,
        hasFocus, row, column);
      if (value == Boolean.TRUE) {
        setIcon(Icons.getIcon("tick"));
      } else {
        setIcon(Icons.getIcon("cross"));
      }
      setText("");
      return component;
    }
  };

  private final ListEx<String> columnNames = Lists.newArray();

  private final Map<String, TimerLabelCountMap> timerByLabel = new ConcurrentHashMap<>();

  private final List<String> labels = new ArrayList<>();

  public TimerLabelCountTableModel(final String labelColumnTitle, String... countLabels) {
    this.columnNames.addValue("Done");
    this.columnNames.addValue(labelColumnTitle);
    for (final var label : countLabels) {
      this.columnNames.addValue(label);
    }
    this.columnNames.addValue("Duration");
  }

  public TimerLabelCountMap addTimer(final String label) {
    var timer = timerByLabel.get(label);
    if (timer == null) {
      timer = Invoke.andWait(() -> {
        final var newTimer = this.timerByLabel.computeIfAbsent(label, newLabel -> {
          this.labels.add(newLabel);
          return new TimerLabelCountMap() {
            @Override
            public void close() {
              super.close();
              Invoke.later(TimerLabelCountTableModel.this::fireTableDataChanged);
            }
          };
        });
        fireTableDataChanged();
        return newTimer;
      });
    }
    return timer;
  }

  public void clear() {
    this.labels.clear();
    this.timerByLabel.clear();
  }

  @Override
  public Class<?> getColumnClass(final int columnIndex) {
    if (columnIndex == 0) {
      return Boolean.class;
    } else if (columnIndex == 1) {
      return String.class;
    } else if (columnIndex == this.labels.size() - 1) {
      return Duration.class;
    } else {
      return Long.class;
    }
  }

  @Override
  public int getColumnCount() {
    return this.columnNames.size();
  }

  @Override
  public String getColumnName(final int columnIndex) {
    return this.columnNames.get(columnIndex);
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
      if (columnIndex == 0) {
        return timerCounter.isClosed();
      } else if (columnIndex == 1) {
        return label;
      } else if (columnIndex == this.columnNames.size() - 1) {
        return timerCounter.duration()
          .truncatedTo(ChronoUnit.SECONDS);
      } else {
        final var name = columnNames.get(columnIndex);
        return timerCounter.getCount(name);
      }
    }
    return null;
  }

  @Override
  public BaseJTable newTable() {
    final var table = super.newTable();
    for (int i = 2; i < columnNames.size(); i++) {
      table.getColumn(i)
        .setMaxWidth(100);
    }
    final TableColumn doneColumn = table.getColumn(0);
    doneColumn.setCellRenderer(BOOLEAN_RENDERER);
    doneColumn.setMaxWidth(60);

    table.setAutoResizeMode(JTable.AUTO_RESIZE_ALL_COLUMNS);
    table.getRowSorter()
      .setSortKeys(
        Lists.newArray(new SortKey(0, SortOrder.ASCENDING), new SortKey(1, SortOrder.ASCENDING)));
    return table;
  }

  @Override
  public void toTsv(final Writer out) {
    try (
      TsvWriter tsv = Tsv.plainWriter(out)) {
      tsv.write(this.columnNames);
      final var row = new ArrayList<>();
      this.timerByLabel.forEach((label, timer) -> {
        row.clear();
        row.add(label);
        for (int i = 0; i < columnNames.size() - 1; i++) {
          final var name = columnNames.get(i);
          final var count = timer.getCount(name);
          row.add(count);
        }
        row.add(timer.duration()
          .truncatedTo(ChronoUnit.SECONDS));
        tsv.write(row);
      });
    }
  }

}
