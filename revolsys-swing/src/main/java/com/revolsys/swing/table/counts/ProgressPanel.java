package com.revolsys.swing.table.counts;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;

import org.jdesktop.swingx.VerticalLayout;

import com.revolsys.date.Dates.Timer;
import com.revolsys.logging.Logs;
import com.revolsys.swing.Borders;
import com.revolsys.swing.TabbedPane;
import com.revolsys.swing.component.BasePanel;
import com.revolsys.swing.listener.EventQueueRunnableListener;
import com.revolsys.swing.logging.ListLoggingAppender;
import com.revolsys.swing.logging.LoggingTableModel;
import com.revolsys.swing.logging.ScopedAppender;
import com.revolsys.swing.parallel.Invoke;
import com.revolsys.swing.table.BaseJTable;
import com.revolsys.swing.table.counts.TimerCounterTableModel.TimerCounter;
import com.revolsys.swing.table.counts.TimerLabelCountTableModel.TimerLabelCountMap;

public class ProgressPanel extends BasePanel {

  private static final long serialVersionUID = 1L;

  private final TimerTableModel timerTableModel = new TimerTableModel();

  private final Map<String, TimerCounterTableModel> timerCounterByLabel = new ConcurrentHashMap<String, TimerCounterTableModel>();

  private final Map<String, TimerLabelCountTableModel> timerLabelCounterByLabel = new ConcurrentHashMap<String, TimerLabelCountTableModel>();

  private final AtomicBoolean timerTabInitialized = new AtomicBoolean();

  private final JLabel statusLabel = new JLabel();

  private final JTabbedPane tabs = new JTabbedPane();

  private ListLoggingAppender appender;

  public ProgressPanel() {
    initDialog();
  }

  protected void addTab(final String label, final BaseJTable table) {
    final var scrollPane = new JScrollPane(table);
    this.tabs.addTab(label, scrollPane);
    this.tabs.setSelectedIndex(this.tabs.getTabCount() - 1);
  }

  protected void initDialog() {
    setLayout(new BorderLayout());

    final JPanel statusPanel = new JPanel(new VerticalLayout());
    this.statusLabel.setSize(new Dimension(400, 50));
    statusPanel.add(this.statusLabel);
    Borders.titled(statusPanel, "Status");
    add(statusPanel, BorderLayout.NORTH);

    final TabbedPane bottomTabs = new TabbedPane();
    this.appender = LoggingTableModel.addNewTabPane(bottomTabs, false);
    bottomTabs.setPreferredSize(new Dimension(600, 150));

    final JSplitPane split = new JSplitPane(JSplitPane.VERTICAL_SPLIT, this.tabs, bottomTabs);
    split.setResizeWeight(1);
    add(split, BorderLayout.CENTER);

  }

  public void run(Runnable action) {
    if (!this.appender.isStarted()) {
      appender.start();
    }
    ScopedAppender.scoped(this.appender)
      .run(() -> {
        final var timer = new javax.swing.Timer(1000,
          new EventQueueRunnableListener(this::repaint));
        timer.start();
        final boolean updateStatus = true;
        try {
          action.run();
        } catch (final Throwable e) {
          Logs.error(this, "Error " + this, e);
        } finally {
          timer.stop();
        }

        if (updateStatus) {
          final StringBuilder status = new StringBuilder("<p><b>Finished running</b></p>");
          setStatus(status.toString());
        }
      });
  }

  public void setSelectedTab(final String tabTitle) {
    Invoke.later(() -> {
      for (int i = 0; i < this.tabs.getTabCount(); i++) {
        final String title = this.tabs.getTitleAt(i);
        if (title.equals(tabTitle)) {
          this.tabs.setSelectedIndex(i);
          return;
        }
      }
    });
  }

  public void setStatus(final CharSequence message) {
    Invoke.andWait(() -> {
      this.statusLabel.setText("<html><body>" + message + "</body></html>");
    });
  }

  public Timer timerAdd(final String label) {
    if (this.timerTabInitialized.compareAndExchange(false, true) == false) {
      Invoke.later(() -> {
        final var table = this.timerTableModel.newTable();
        addTab("Timer", table);
      });
    }
    return this.timerTableModel.addTimer(label);
  }

  public TimerCounter timerCounterAdd(final String tabName, final String labelColumnName,
    final String label) {
    final var modelRef = new AtomicReference<TimerCounterTableModel>();
    final var timers = this.timerCounterByLabel.computeIfAbsent(tabName, _ -> {
      final var tableModel = new TimerCounterTableModel(labelColumnName);
      modelRef.set(tableModel);
      return tableModel;
    });
    // Add the tab if a new table model was created
    if (modelRef.get() == timers) {
      Invoke.later(() -> {
        final var table = timers.newTable();
        addTab(tabName, table);
      });
    }
    return timers.addTimer(label);
  }

  public TimerLabelCountMap timerLabelCounterAdd(final String tabName, final String labelColumnName,
    final String label, String... countNames) {
    final var modelRef = new AtomicReference<TimerLabelCountTableModel>();
    final var timers = this.timerLabelCounterByLabel.computeIfAbsent(tabName, _ -> {
      final var tableModel = new TimerLabelCountTableModel(labelColumnName, countNames);
      modelRef.set(tableModel);
      return tableModel;
    });
    // Add the tab if a new table model was created
    if (modelRef.get() == timers) {
      Invoke.later(() -> {
        final var table = timers.newTable();
        addTab(tabName, table);
      });
    }
    return timers.addTimer(label);
  }

}
