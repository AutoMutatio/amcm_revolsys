package com.revolsys.swing.table.counts;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.WindowConstants;

import org.jdesktop.swingx.VerticalLayout;

import com.revolsys.date.Dates.Timer;
import com.revolsys.log.LogbackUtil;
import com.revolsys.logging.Logs;
import com.revolsys.swing.Borders;
import com.revolsys.swing.SwingUtil;
import com.revolsys.swing.TabbedPane;
import com.revolsys.swing.action.RunnableAction;
import com.revolsys.swing.component.BaseDialog;
import com.revolsys.swing.listener.EventQueueRunnableListener;
import com.revolsys.swing.logging.LoggingTableModel;
import com.revolsys.swing.logging.SwingAlertAppender;
import com.revolsys.swing.parallel.BackgroundTaskTableModel;
import com.revolsys.swing.parallel.Invoke;
import com.revolsys.swing.table.BaseJTable;
import com.revolsys.swing.table.counts.TimerCounterTableModel.TimerCounter;
import com.revolsys.util.Cancellable;
import com.revolsys.util.CaseConverter;
import com.revolsys.util.Property;

public class ProgressDialog extends BaseDialog implements WindowListener, Runnable, Cancellable {

  private static final long serialVersionUID = 1L;

  private JButton cancelButton;

  private boolean cancelled;

  private final TimerTableModel timerTableModel = new TimerTableModel();

  private final Map<String, TimerCounterTableModel> timerCounterByLabel = new ConcurrentHashMap<String, TimerCounterTableModel>();

  private final AtomicBoolean timerTabInitialized = new AtomicBoolean();

  private boolean finished;

  private JButton okButton;

  private final Runnable action;

  private final JLabel statusLabel = new JLabel();

  private final JTabbedPane tabs = new JTabbedPane();

  public ProgressDialog(String title, Runnable action) {
    super(title, ModalityType.DOCUMENT_MODAL);
    if (!Property.hasValue(title)) {
      title = CaseConverter.toCapitalizedWords(getClass().getSimpleName());
      setTitle(title);
    }
    SwingUtil.setSplashTitle(title);
    initDialog();
    this.action = action;
  }

  protected void addTab(final String label, final BaseJTable table) {
    final var scrollPane = new JScrollPane(table);
    this.tabs.addTab(label, scrollPane);
  }

  @Override
  public void cancel() {
    if (this.finished) {
      finish();
    } else {
      this.cancelled = true;
      cancelDo();
    }
  }

  protected void cancelDo() {
  }

  public void finish() {
    if (isVisible()) {
      SwingUtil.dispose(this);
    }
  }

  protected void initDialog() {
    final var logAppender = new SwingAlertAppender();
    LogbackUtil.addRootAppender(logAppender);

    setDefaultCloseOperation(WindowConstants.DO_NOTHING_ON_CLOSE);
    addWindowListener(this);
    setLayout(new BorderLayout());

    final JPanel statusPanel = new JPanel(new VerticalLayout());
    this.statusLabel.setSize(new Dimension(400, 50));
    statusPanel.add(this.statusLabel);
    Borders.titled(statusPanel, "Status");
    add(statusPanel, BorderLayout.NORTH);

    final TabbedPane bottomTabs = new TabbedPane();
    BackgroundTaskTableModel.addNewTabPanel(bottomTabs);
    LoggingTableModel.addNewTabPane(bottomTabs);
    bottomTabs.setPreferredSize(new Dimension(600, 150));

    final JSplitPane split = new JSplitPane(JSplitPane.VERTICAL_SPLIT, this.tabs, bottomTabs);
    split.setResizeWeight(1);
    add(split, BorderLayout.CENTER);

    final JPanel buttonsPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT));
    add(buttonsPanel, BorderLayout.SOUTH);

    this.cancelButton = RunnableAction.newButton("Cancel", this::cancel);
    buttonsPanel.add(this.cancelButton);

    this.okButton = RunnableAction.newButton("OK", this::finish);
    this.okButton.setEnabled(false);
    buttonsPanel.add(this.okButton);

    SwingUtil.fillScreen(this);
  }

  @Override
  public boolean isCancelled() {
    return this.cancelled;
  }

  protected void resizeWindow() {
    SwingUtil.autoAdjustSize(ProgressDialog.this);
  }

  @Override
  public void run() {
    final var timer = new javax.swing.Timer(1000, new EventQueueRunnableListener(this::repaint));
    timer.start();
    final boolean updateStatus = true;
    try {
      action.run();
    } catch (final Throwable e) {
      Logs.error(this, "Error " + this, e);
    } finally {
      this.cancelButton.setEnabled(false);
      this.okButton.setEnabled(true);
      this.finished = true;
      timer.stop();
    }

    if (updateStatus) {
      final StringBuilder status = new StringBuilder("<p><b>Finished running ");
      status.append(this);
      status.append("</b></p>");
      if (this.cancelled) {
        status.append("<p style=\"color:red;font-weight: bold\">The process was cancelled.</p>");
      }
      status.append("<p>Click OK to close this dialog and continue.</p>");
      setStatus(status.toString());
    }
  }

  protected void setOKButtonTitle(final String title) {
    Invoke.later(() -> {
      if (Property.hasValue(title)) {
        this.okButton.setText(title);
      } else {
        this.okButton.setText("OK");
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
    Invoke.later(() -> {
      this.statusLabel.setText("<html><body>" + message + "</body></html>");
    });
  }

  public void showDialog() {
    Invoke.later(this::showDialogDo);
  }

  protected void showDialogDo() {
    Invoke.background(toString(), this);
    setVisible(true);
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

  @Override
  public String toString() {
    return getTitle();
  }

  @Override
  public void windowActivated(final WindowEvent e) {
  }

  @Override
  public void windowClosed(final WindowEvent e) {
  }

  @Override
  public void windowClosing(final WindowEvent e) {
    cancel();
  }

  @Override
  public void windowDeactivated(final WindowEvent e) {
  }

  @Override
  public void windowDeiconified(final WindowEvent e) {
  }

  @Override
  public void windowIconified(final WindowEvent e) {
  }

  @Override
  public void windowOpened(final WindowEvent e) {
  }
}
