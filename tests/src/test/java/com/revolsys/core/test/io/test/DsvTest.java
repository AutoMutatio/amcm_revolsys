package com.revolsys.core.test.io.test;

import java.io.StringWriter;
import java.util.Arrays;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.revolsys.record.io.format.csv.DsvWriter;

class DsvTest {

  private final String longText = """
Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed hendrerit, eros ac dictum euismod, turpis metus malesuada felis, vel placerat sapien ex id erat. Mauris ultrices, diam quis vehicula congue, massa nisi volutpat purus, et interdum ex leo ut odio. Maecenas scelerisque gravida massa nec semper. Quisque mollis dictum libero. Duis eu pretium ante. Sed et felis justo. Nam volutpat tincidunt mauris et faucibus.

Proin vitae nisl sit amet augue tincidunt aliquet. Vestibulum fermentum augue molestie, rhoncus tortor eget, finibus diam. Nulla posuere augue in porttitor lobortis. Integer sit amet convallis dolor. Ut fermentum dolor sapien, et feugiat eros porttitor nec. Duis tempus velit vitae ultrices dignissim. Cras egestas porttitor nisi in pulvinar. Vestibulum placerat metus at aliquam placerat. Interdum et malesuada fames ac ante ipsum primis in faucibus. Suspendisse auctor eleifend nisi, vitae rhoncus elit lobortis eu. Vivamus facilisis massa mauris, a placerat nisl tempor ac. Vivamus sagittis tincidunt risus sit amet viverra. Vestibulum consequat pretium posuere. Vivamus velit mi, iaculis at sapien non, malesuada cursus metus. Mauris vel tellus et nisi bibendum ullamcorper. Nulla consectetur mauris vitae molestie vulputate.

Aliquam erat volutpat. Curabitur vel elit vehicula, faucibus justo a, lobortis orci. Praesent at luctus quam. Nunc lectus magna, lobortis et egestas ut, viverra eu lorem. Proin eget aliquet mauris, in hendrerit justo. Curabitur et tellus mauris. Duis nisi sem, rutrum quis libero eget, dictum lacinia sem. Integer consequat elementum sem, a laoreet turpis facilisis at. Phasellus et viverra enim, nec ornare massa. Pellentesque ut venenatis sem, ut rutrum urna. Aliquam faucibus fringilla lectus, at ornare leo vehicula id. Etiam sed nulla quam. Curabitur vitae quam euismod, luctus enim in, commodo nibh. Etiam ac eros vel felis ullamcorper aliquam. Cras at placerat mi.

Curabitur hendrerit ligula a mauris placerat, sed mattis eros molestie. Vivamus sit amet vestibulum mi, ac faucibus mi turpis duis.
""";

  public void doQuoted(final String inString, final String expectedOut) {
    final StringWriter outText = new StringWriter();

    try (
      var writer = new DsvWriter(outText, ',')) {
      writer.quotedString(inString);
    }
    Assertions.assertEquals(expectedOut, outText.toString());
  }

  public void doRaw(final String inString, final String expectedOut) {
    final StringWriter outText = new StringWriter();

    try (
      var writer = new DsvWriter(outText, ',')) {
      writer.rawString(inString);
    }
    Assertions.assertEquals(expectedOut, outText.toString());
  }

  @Test
  void escaped() {
    doQuoted(null, "");
    doQuoted("", "");
    doQuoted("hello", "\"hello\"");
    doQuoted("\"Paul\"", "\"\"\"Paul\"\"\"");
    doQuoted("hi \"Paul\"", "\"hi \"\"Paul\"\"\"");
    doQuoted("hi \"Paul\" how are you", "\"hi \"\"Paul\"\" how are you\"");

    for (final var length : Arrays.asList(1023, 1024, 1025, 2047, 2048, 2049)) {
      final var t = this.longText.substring(0, length);
      doQuoted(t, '"' + t + '"');

      final var t2 = this.longText.substring(0, length) + '"';
      doQuoted(t2, '"' + t2.replaceAll("\"", "\"\"") + '"');
    }
  }

  @Test
  void raw() {
    for (final var length : Arrays.asList(1023, 1024, 1025, 2047, 2048, 2049)) {
      final var t = this.longText.substring(0, length);
      doRaw(t, t);
    }
  }
}
