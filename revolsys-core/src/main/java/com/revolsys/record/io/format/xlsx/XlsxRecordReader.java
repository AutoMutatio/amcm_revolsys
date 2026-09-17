package com.revolsys.record.io.format.xlsx;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import org.docx4j.openpackaging.exceptions.Docx4JException;
import org.docx4j.openpackaging.packages.OpcPackage;
import org.docx4j.openpackaging.packages.SpreadsheetMLPackage;
import org.docx4j.openpackaging.parts.DocPropsCustomPart;
import org.docx4j.openpackaging.parts.SpreadsheetML.SharedStrings;
import org.docx4j.openpackaging.parts.SpreadsheetML.WorkbookPart;
import org.docx4j.openpackaging.parts.SpreadsheetML.WorksheetPart;
import org.xlsx4j.exceptions.Xlsx4jException;
import org.xlsx4j.sml.CTRElt;
import org.xlsx4j.sml.CTRst;
import org.xlsx4j.sml.CTSst;
import org.xlsx4j.sml.CTStylesheet;
import org.xlsx4j.sml.CTXf;
import org.xlsx4j.sml.CTXstringWhitespace;
import org.xlsx4j.sml.Cell;
import org.xlsx4j.sml.Row;
import org.xlsx4j.sml.STCellType;
import org.xlsx4j.sml.Sheet;
import org.xlsx4j.sml.SheetData;
import org.xlsx4j.sml.Worksheet;

import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;
import com.revolsys.collection.map.MapEx;
import com.revolsys.exception.ExceptionWithProperties;
import com.revolsys.geometry.model.GeometryFactory;
import com.revolsys.record.ArrayRecord;
import com.revolsys.record.Record;
import com.revolsys.record.RecordFactory;
import com.revolsys.record.io.AbstractRecordReader;
import com.revolsys.spring.resource.Resource;

public class XlsxRecordReader extends AbstractRecordReader {

  private static final LocalDateTime DATE_TIME_EPOCH = LocalDateTime.of(1900, 1, 1, 0, 0);

  private static final LocalDate DATE_EPOCH = LocalDate.of(1900, 1, 1);

  private static BigDecimal NANOSECONDS_PER_DAY = BigDecimal.valueOf(60 * 60 * 24 * 1e9);

  private static final BigDecimal BD_MILISEC_RND = BigDecimal.valueOf(0.5 * 1e6);

  public static int getColumnIndex(final Cell cell) {
    final String cellReference = cell.getR();
    if (cellReference == null) {
      return -1;
    } else {
      int columnIndex = 0;
      for (int i = 0; i < cellReference.length(); i++) {
        final char character = cellReference.charAt(i);
        if (character >= 'A' && character <= 'Z') {
          columnIndex *= 26;
          columnIndex += character - 'A' + 1;
        } else {
          return columnIndex - 1;
        }
      }
      return columnIndex - 1;
    }
  }

  private int rowOffset;

  /**
   * The offset to the first column. Used where the table doesn't start in the first column
   */
  private int columnOffset;

  private Resource resource;

  private List<Row> rows = Collections.emptyList();

  /**
   * Offset to the first row. Used where the table doesn't start at the first row.
   */
  private int rowIndex = 0;

  private List<CTRst> sharedStringList = Collections.emptyList();

  private String tabName;

  private List<String> fieldNames;

  private List<CTXf> cellXfs;

  private CTStylesheet stylesheet;

  public XlsxRecordReader(final Resource resource) {
    this(resource, ArrayRecord.FACTORY);
  }

  public XlsxRecordReader(final Resource resource,
    final RecordFactory<? extends Record> recordFactory) {
    super(recordFactory);
    this.resource = resource;
  }

  public XlsxRecordReader(final Resource resource,
    final RecordFactory<? extends Record> recordFactory, final MapEx properties) {
    this(resource, recordFactory);
    setProperties(properties);
  }

  @Override
  protected void closeDo() {
    super.closeDo();
    this.resource = null;
    this.rows = Collections.emptyList();
    this.sharedStringList = Collections.emptyList();
  }

  public int getColumnOffset() {
    return this.columnOffset;
  }

  @Override
  protected Record getNext() {
    final var row = readNextRow();
    if (row != null && row.size() > 0) {
      return parseRecord(this.fieldNames, row);
    } else {
      throw new NoSuchElementException();
    }
  }

  public int getRowOffset() {
    return this.rowOffset;
  }

  protected String getText(final CTRst sharedString) {
    final CTXstringWhitespace text = sharedString.getT();
    if (text == null) {
      final List<CTRElt> r = sharedString.getR();
      if (r != null) {
        final StringBuilder t = new StringBuilder();
        for (final CTRElt e : r) {
          t.append(e.getT()
            .getValue());
        }
        return t.toString();
      }
      return "";
    } else {
      return text.getValue();
    }
  }

  private WorksheetPart getWorksheetPart(final WorkbookPart workbook) throws Xlsx4jException {
    if (this.tabName == null) {
      return workbook.getWorksheet(0);
    } else {
      int i = 0;
      for (final Sheet sheet : workbook.getJaxbElement()
        .getSheets()
        .getSheet()) {
        if (sheet.getName()
          .equals(this.tabName)) {
          return workbook.getWorksheet(i);
        }
        i++;
      }
      return null;
    }
  }

  @Override
  protected void initDo() {
    super.initDo();
    this.rowIndex = this.rowOffset; // skip to first row
    try (
      InputStream in = this.resource.newBufferedInputStream()) {

      final SpreadsheetMLPackage spreadsheetPackage = (SpreadsheetMLPackage)OpcPackage.load(in);
      final DocPropsCustomPart customProperties = spreadsheetPackage.getDocPropsCustomPart();
      if (customProperties != null) {
        int srid = 0;
        try {
          srid = Integer.parseInt(customProperties.getProperty("srid")
            .getLpwstr());
        } catch (final Throwable e) {
        }
        int axisCount = 2;
        try {
          axisCount = Integer.parseInt(customProperties.getProperty("axisCount")
            .getLpwstr());
          if (axisCount > 4) {
            axisCount = 2;
          }
        } catch (final Throwable e) {
        }
        double scaleXy = 0;
        try {
          scaleXy = Double.parseDouble(customProperties.getProperty("scaleXy")
            .getLpwstr());
        } catch (final Throwable e) {
        }
        double scaleZ = 0;
        try {
          scaleZ = Double.parseDouble(customProperties.getProperty("scaleZ")
            .getLpwstr());
        } catch (final Throwable e) {
        }
        final GeometryFactory geometryFactory = GeometryFactory.fixed(srid, axisCount, scaleXy,
          scaleXy, scaleZ);
        setGeometryFactory(geometryFactory);
      }
      final WorkbookPart workbook = spreadsheetPackage.getWorkbookPart();
      final SharedStrings sharedStrings = workbook.getSharedStrings();
      if (sharedStrings != null) {
        final CTSst contents = sharedStrings.getContents();
        this.sharedStringList = contents.getSi();
      }
      final WorksheetPart worksheetPart = getWorksheetPart(workbook);

      if (worksheetPart != null) {
        final Worksheet worksheet = worksheetPart.getContents();
        final SheetData sheetData = worksheet.getSheetData();
        this.rows = sheetData.getRow();
        this.stylesheet = workbook.getStylesPart()
          .getContents();
        this.cellXfs = this.stylesheet.getCellXfs()
          .getXf();

        this.fieldNames = readNextRow().map(Object::toString)
          .toList();
        final String baseName = this.resource.getBaseName();
        newRecordDefinition(baseName, this.fieldNames);
      }
    } catch (final IOException | Docx4JException | Xlsx4jException e) {
      throw new ExceptionWithProperties(e).property("resource", this.resource.toString());
    } catch (final NoSuchElementException e) {
    }
  }

  @Override
  protected GeometryFactory loadGeometryFactory() {
    return GeometryFactory.floating2d(this.resource);
  }

  /**
   * Reads the next line from the buffer and converts to a string array.
   *
   * @return a string array with each comma-separated element as a separate
   *         entry.
   * @throws IOException if bad things happen during the read
   */
  private ListEx<Object> readNextRow() {
    if (this.rowIndex < this.rows.size()) {
      final var values = Lists.newArray();
      final Row row = this.rows.get(this.rowIndex);
      final List<Cell> cells = row.getC();
      for (final Cell cell : cells) {
        final String cellValue = cell.getV();
        final var styleIndex = cell.getS();

        final STCellType cellType = cell.getT();
        final Object value = switch (cellType) {
          case B -> "1".equals(cellValue) || "true".equalsIgnoreCase(cellValue);
          case S -> {
            final int stringIndex = Integer.parseInt(cellValue);
            final CTRst sharedString = this.sharedStringList.get(stringIndex);
            yield getText(sharedString);
          }
          case N -> {
            final CTXf xf = this.cellXfs.get((int)styleIndex);
            boolean isDate = false;
            if (xf != null) {
              final long numFmtId = xf.getNumFmtId() != null ? xf.getNumFmtId() : 0L;

              if (numFmtId >= 14 && numFmtId <= 22 || numFmtId >= 27 && numFmtId <= 36
                || numFmtId >= 45 && numFmtId <= 47) {
                isDate = true;
              }
              if (this.stylesheet.getNumFmts() != null) {
                for (final var numFmt : this.stylesheet.getNumFmts()
                  .getNumFmt()) {
                  if (numFmt.getNumFmtId() == numFmtId) {
                    final String formatStr = numFmt.getFormatCode()
                      .toLowerCase();
                    // Search for common date/time identifier characters
                    isDate |= (formatStr.contains("y") || formatStr.contains("m")
                      || formatStr.contains("d") || formatStr.contains("h"))
                      && !formatStr.contains("#") && !formatStr.contains("0");
                  }
                }
              }
            }
            if (isDate) {
              if (cellValue == null) {
                yield null;
              }
              final var bd = new BigDecimal(cellValue);

              final int wholeDays = bd.intValue();

              int dayAdjust = -1;
              if (wholeDays < 61) {
                dayAdjust = 0;
              }
              final var nanos = bd.subtract(BigDecimal.valueOf(wholeDays))
                .multiply(NANOSECONDS_PER_DAY);
              final long nanosTime = nanos.add(BD_MILISEC_RND)
                .longValue();
              if (nanos.compareTo(BigDecimal.ZERO) == 0) {
                yield DATE_EPOCH.plusDays(wholeDays + dayAdjust - 1L);
              } else {
                yield DATE_TIME_EPOCH.plusDays(wholeDays + dayAdjust - 1L)
                  .plusNanos(nanosTime)
                  .truncatedTo(ChronoUnit.MILLIS);
              }
            }
            if (cellValue == null) {
              yield null;
            }
            yield new BigDecimal(cellValue);
          }
          case INLINE_STR -> {
            final CTRst is = cell.getIs();
            if (is == null) {
              yield cellValue;
            } else {
              yield is.getT()
                .getValue();
            }
          }
          default -> {
            if (cellValue == null) {
              final CTRst is = cell.getIs();
              if (is == null) {
                yield null;
              } else {
                yield is.getT()
                  .getValue();
              }
            } else {
              yield cellValue;
            }
          }
        };
        int columnIndex = getColumnIndex(cell);
        if (columnIndex == -1) {
          values.add(value);
        } else {
          if (this.columnOffset != 0) {
            columnIndex -= this.columnOffset;
          }
          while (values.size() < columnIndex) {
            values.add(null);
          }
          values.add(columnIndex, value);
        }
      }
      this.rowIndex++;
      return values;
    } else {
      throw new NoSuchElementException();
    }
  }

  public void setColumnOffset(final int columnOffset) {
    this.columnOffset = columnOffset;
  }

  public void setRowOffset(final int rowOffset) {
    this.rowOffset = rowOffset;
  }

  public void setTabName(final String tabName) {
    this.tabName = tabName;
  }
}
