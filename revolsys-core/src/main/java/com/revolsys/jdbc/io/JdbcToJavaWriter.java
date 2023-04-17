package com.revolsys.jdbc.io;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import com.revolsys.io.BaseCloseable;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.util.Strings;

public abstract class JdbcToJavaWriter implements BaseCloseable {
  private final PrintWriter out;

  public JdbcToJavaWriter(final String packageName, final Path file) throws IOException {
    this.out = new PrintWriter(Files.newBufferedWriter(file, StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING));
    this.out.print("package ");
    this.out.print(packageName);
    this.out.println(";");

    this.out.println("import com.revolsys.record.schema.*;");
  }

  @Override
  public void close() {
    this.out.flush();
    this.out.close();
  }

  public PrintWriter getOut() {
    return this.out;
  }

  public void println() {
    this.out.println();
  }

  public abstract void writeColumnDataType(final FieldDefinition field);

  protected void writeColumnNames(final List<String> names) {
    boolean first = true;
    for (final String idFieldName : names) {
      if (first) {
        first = false;
      } else {
        this.out.print(",");
      }
      this.out.print(idFieldName);
    }
  }

  private void writeField(final PrintWriter out, final FieldDefinition field) {
    final String name = field.getName();
    out.println("     .addField(field->field //");
    out.print("       .setName(\"");
    out.print(name);
    out.println("\")");
    out.print("       .setType(\"");
    out.print(field.getDataType().getName());
    out.print("\")");
    final int length = field.getLength();
    if (length > 0) {
      out.println();
      out.print("       .setLength(");
      out.print(length);
      out.print(")");
    }
    final int scale = field.getScale();
    if (scale > 0) {
      out.println();
      out.print("       .setScale(");
      out.print(scale);
      out.print(")");
    }
    if (field.isRequired()) {
      out.println();
      out.print("       .setRequired(true)");
    }
    if (field.isGenerated()) {
      out.println();
      out.println("       .setGenerateStatement(\"\"\"");
      final String statement = field.getGenerateStatement();
      out.println(statement);
      out.print("\"\"\")");
    } else {
      final String defaultStatement = field.getDefaultStatement();
      if (defaultStatement != null) {
        out.println();
        out.println("       .setDefaultStatement(\"\"\"");
        out.println(defaultStatement);
        out.print("\"\"\")");
      }
    }
    out.println(" //");
    out.println("      )");
  }

  public void writeGrant(final String typePath, final String username, final boolean select,
    final boolean insert, final boolean update, final boolean delete) {

    this.out.print("GRANT ");
    final List<String> perms = new ArrayList<>();
    if (select) {
      perms.add("SELECT");
    }
    if (insert) {
      perms.add("INSERT");
    }
    if (update) {
      perms.add("UPDATE");
    }
    if (delete) {
      perms.add("DELETE");
    }
    this.out.print(Strings.toString(", ", perms));
    this.out.print(" ON ");
    // writeTableName(typePath);
    this.out.print(" TO ");
    this.out.print(username);
    this.out.println(";");

  }

  public void writeRecordDefinition(final RecordDefinition recordDefinition) {
    final PrintWriter out = this.out;
    out.println();
    out.print("public class ");
    out.print(recordDefinition.getName());
    out.println(" {");
    out.println();
    out.print("  public ");
    out.print(recordDefinition.getName());
    out.println("() {");
    out.println(" }");

    out.println("  public RecordDefinition get() {");
    out.print("    RecordDefinitionBuilder recordDefinition = new RecordDefinitionBuilder(\"");
    out.print(recordDefinition.getPathName());
    out.println("\") // ");
    for (final FieldDefinition field : recordDefinition.getFields()) {
      writeField(out, field);
    }
    final List<String> idFieldNames = recordDefinition.getIdFieldNames();
    if (!idFieldNames.isEmpty()) {
      out.print("      .setIdFieldNames(\"");
      out.print(Strings.toString("\", \"", idFieldNames));
      out.println("\")");
    }

    writeRecordDefinitionAfter(recordDefinition);
    out.println(";");
    out.println("    return recordDefinition.getRecordDefinition();");
    out.println("  }");

    out.println("}");

  }

  protected void writeRecordDefinitionAfter(final RecordDefinition recordDefinition) {

  }

}
