package com.revolsys.swing.table.counts;

import com.revolsys.swing.table.counts.TimerLabelCountTableModel.TimerLabelCountMap;

public interface ProgressMonitor {
  TimerLabelCountMap timerLabelCounterAdd(final String tabName, final String labelColumnName,
    final String label, final String... countNames);
}
