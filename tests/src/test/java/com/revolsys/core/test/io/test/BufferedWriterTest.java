package com.revolsys.core.test.io.test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.revolsys.record.io.BufferedWriterEx;
import com.revolsys.util.Strings;

class BufferedWriterTest {

  private final String longText = """
Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed hendrerit, eros ac dictum euismod, turpis metus malesuada felis, vel placerat sapien ex id erat. Mauris ultrices, diam quis vehicula congue, massa nisi volutpat purus, et interdum ex leo ut odio. Maecenas scelerisque gravida massa nec semper. Quisque mollis dictum libero. Duis eu pretium ante. Sed et felis justo. Nam volutpat tincidunt mauris et faucibus.

Proin vitae nisl sit amet augue tincidunt aliquet. Vestibulum fermentum augue molestie, rhoncus tortor eget, finibus diam. Nulla posuere augue in porttitor lobortis. Integer sit amet convallis dolor. Ut fermentum dolor sapien, et feugiat eros porttitor nec. Duis tempus velit vitae ultrices dignissim. Cras egestas porttitor nisi in pulvinar. Vestibulum placerat metus at aliquam placerat. Interdum et malesuada fames ac ante ipsum primis in faucibus. Suspendisse auctor eleifend nisi, vitae rhoncus elit lobortis eu. Vivamus facilisis massa mauris, a placerat nisl tempor ac. Vivamus sagittis tincidunt risus sit amet viverra. Vestibulum consequat pretium posuere. Vivamus velit mi, iaculis at sapien non, malesuada cursus metus. Mauris vel tellus et nisi bibendum ullamcorper. Nulla consectetur mauris vitae molestie vulputate.

Aliquam erat volutpat. Curabitur vel elit vehicula, faucibus justo a, lobortis orci. Praesent at luctus quam. Nunc lectus magna, lobortis et egestas ut, viverra eu lorem. Proin eget aliquet mauris, in hendrerit justo. Curabitur et tellus mauris. Duis nisi sem, rutrum quis libero eget, dictum lacinia sem. Integer consequat elementum sem, a laoreet turpis facilisis at. Phasellus et viverra enim, nec ornare massa. Pellentesque ut venenatis sem, ut rutrum urna. Aliquam faucibus fringilla lectus, at ornare leo vehicula id. Etiam sed nulla quam. Curabitur vitae quam euismod, luctus enim in, commodo nibh. Etiam ac eros vel felis ullamcorper aliquam. Cras at placerat mi.

Curabitur hendrerit ligula a mauris placerat, sed mattis eros molestie. Vivamus sit amet vestibulum mi, ac faucibus mi turpis duis.
Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed hendrerit, eros ac dictum euismod, turpis metus malesuada felis, vel placerat sapien ex id erat. Mauris ultrices, diam quis vehicula congue, massa nisi volutpat purus, et interdum ex leo ut odio. Maecenas scelerisque gravida massa nec semper. Quisque mollis dictum libero. Duis eu pretium ante. Sed et felis justo. Nam volutpat tincidunt mauris et faucibus.

Proin vitae nisl sit amet augue tincidunt aliquet. Vestibulum fermentum augue molestie, rhoncus tortor eget, finibus diam. Nulla posuere augue in porttitor lobortis. Integer sit amet convallis dolor. Ut fermentum dolor sapien, et feugiat eros porttitor nec. Duis tempus velit vitae ultrices dignissim. Cras egestas porttitor nisi in pulvinar. Vestibulum placerat metus at aliquam placerat. Interdum et malesuada fames ac ante ipsum primis in faucibus. Suspendisse auctor eleifend nisi, vitae rhoncus elit lobortis eu. Vivamus facilisis massa mauris, a placerat nisl tempor ac. Vivamus sagittis tincidunt risus sit amet viverra. Vestibulum consequat pretium posuere. Vivamus velit mi, iaculis at sapien non, malesuada cursus metus. Mauris vel tellus et nisi bibendum ullamcorper. Nulla consectetur mauris vitae molestie vulputate.

Aliquam erat volutpat. Curabitur vel elit vehicula, faucibus justo a, lobortis orci. Praesent at luctus quam. Nunc lectus magna, lobortis et egestas ut, viverra eu lorem. Proin eget aliquet mauris, in hendrerit justo. Curabitur et tellus mauris. Duis nisi sem, rutrum quis libero eget, dictum lacinia sem. Integer consequat elementum sem, a laoreet turpis facilisis at. Phasellus et viverra enim, nec ornare massa. Pellentesque ut venenatis sem, ut rutrum urna. Aliquam faucibus fringilla lectus, at ornare leo vehicula id. Etiam sed nulla quam. Curabitur vitae quam euismod, luctus enim in, commodo nibh. Etiam ac eros vel felis ullamcorper aliquam. Cras at placerat mi.

Curabitur hendrerit ligula a mauris placerat, sed mattis eros molestie. Vivamus sit amet vestibulum mi, ac faucibus mi turpis duis.
Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed hendrerit, eros ac dictum euismod, turpis metus malesuada felis, vel placerat sapien ex id erat. Mauris ultrices, diam quis vehicula congue, massa nisi volutpat purus, et interdum ex leo ut odio. Maecenas scelerisque gravida massa nec semper. Quisque mollis dictum libero. Duis eu pretium ante. Sed et felis justo. Nam volutpat tincidunt mauris et faucibus.

Proin vitae nisl sit amet augue tincidunt aliquet. Vestibulum fermentum augue molestie, rhoncus tortor eget, finibus diam. Nulla posuere augue in porttitor lobortis. Integer sit amet convallis dolor. Ut fermentum dolor sapien, et feugiat eros porttitor nec. Duis tempus velit vitae ultrices dignissim. Cras egestas porttitor nisi in pulvinar. Vestibulum placerat metus at aliquam placerat. Interdum et malesuada fames ac ante ipsum primis in faucibus. Suspendisse auctor eleifend nisi, vitae rhoncus elit lobortis eu. Vivamus facilisis massa mauris, a placerat nisl tempor ac. Vivamus sagittis tincidunt risus sit amet viverra. Vestibulum consequat pretium posuere. Vivamus velit mi, iaculis at sapien non, malesuada cursus metus. Mauris vel tellus et nisi bibendum ullamcorper. Nulla consectetur mauris vitae molestie vulputate.

Aliquam erat volutpat. Curabitur vel elit vehicula, faucibus justo a, lobortis orci. Praesent at luctus quam. Nunc lectus magna, lobortis et egestas ut, viverra eu lorem. Proin eget aliquet mauris, in hendrerit justo. Curabitur et tellus mauris. Duis nisi sem, rutrum quis libero eget, dictum lacinia sem. Integer consequat elementum sem, a laoreet turpis facilisis at. Phasellus et viverra enim, nec ornare massa. Pellentesque ut venenatis sem, ut rutrum urna. Aliquam faucibus fringilla lectus, at ornare leo vehicula id. Etiam sed nulla quam. Curabitur vitae quam euismod, luctus enim in, commodo nibh. Etiam ac eros vel felis ullamcorper aliquam. Cras at placerat mi.

Curabitur hendrerit ligula a mauris placerat, sed mattis eros molestie. Vivamus sit amet vestibulum mi, ac faucibus mi turpis duis.
Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed hendrerit, eros ac dictum euismod, turpis metus malesuada felis, vel placerat sapien ex id erat. Mauris ultrices, diam quis vehicula congue, massa nisi volutpat purus, et interdum ex leo ut odio. Maecenas scelerisque gravida massa nec semper. Quisque mollis dictum libero. Duis eu pretium ante. Sed et felis justo. Nam volutpat tincidunt mauris et faucibus.

Proin vitae nisl sit amet augue tincidunt aliquet. Vestibulum fermentum augue molestie, rhoncus tortor eget, finibus diam. Nulla posuere augue in porttitor lobortis. Integer sit amet convallis dolor. Ut fermentum dolor sapien, et feugiat eros porttitor nec. Duis tempus velit vitae ultrices dignissim. Cras egestas porttitor nisi in pulvinar. Vestibulum placerat metus at aliquam placerat. Interdum et malesuada fames ac ante ipsum primis in faucibus. Suspendisse auctor eleifend nisi, vitae rhoncus elit lobortis eu. Vivamus facilisis massa mauris, a placerat nisl tempor ac. Vivamus sagittis tincidunt risus sit amet viverra. Vestibulum consequat pretium posuere. Vivamus velit mi, iaculis at sapien non, malesuada cursus metus. Mauris vel tellus et nisi bibendum ullamcorper. Nulla consectetur mauris vitae molestie vulputate.

Aliquam erat volutpat. Curabitur vel elit vehicula, faucibus justo a, lobortis orci. Praesent at luctus quam. Nunc lectus magna, lobortis et egestas ut, viverra eu lorem. Proin eget aliquet mauris, in hendrerit justo. Curabitur et tellus mauris. Duis nisi sem, rutrum quis libero eget, dictum lacinia sem. Integer consequat elementum sem, a laoreet turpis facilisis at. Phasellus et viverra enim, nec ornare massa. Pellentesque ut venenatis sem, ut rutrum urna. Aliquam faucibus fringilla lectus, at ornare leo vehicula id. Etiam sed nulla quam. Curabitur vitae quam euismod, luctus enim in, commodo nibh. Etiam ac eros vel felis ullamcorper aliquam. Cras at placerat mi.

Curabitur hendrerit ligula a mauris placerat, sed mattis eros molestie. Vivamus sit amet vestibulum mi, ac faucibus mi turpis duis.
Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed hendrerit, eros ac dictum euismod, turpis metus malesuada felis, vel placerat sapien ex id erat. Mauris ultrices, diam quis vehicula congue, massa nisi volutpat purus, et interdum ex leo ut odio. Maecenas scelerisque gravida massa nec semper. Quisque mollis dictum libero. Duis eu pretium ante. Sed et felis justo. Nam volutpat tincidunt mauris et faucibus.

Proin vitae nisl sit amet augue tincidunt aliquet. Vestibulum fermentum augue molestie, rhoncus tortor eget, finibus diam. Nulla posuere augue in porttitor lobortis. Integer sit amet convallis dolor. Ut fermentum dolor sapien, et feugiat eros porttitor nec. Duis tempus velit vitae ultrices dignissim. Cras egestas porttitor nisi in pulvinar. Vestibulum placerat metus at aliquam placerat. Interdum et malesuada fames ac ante ipsum primis in faucibus. Suspendisse auctor eleifend nisi, vitae rhoncus elit lobortis eu. Vivamus facilisis massa mauris, a placerat nisl tempor ac. Vivamus sagittis tincidunt risus sit amet viverra. Vestibulum consequat pretium posuere. Vivamus velit mi, iaculis at sapien non, malesuada cursus metus. Mauris vel tellus et nisi bibendum ullamcorper. Nulla consectetur mauris vitae molestie vulputate.

Aliquam erat volutpat. Curabitur vel elit vehicula, faucibus justo a, lobortis orci. Praesent at luctus quam. Nunc lectus magna, lobortis et egestas ut, viverra eu lorem. Proin eget aliquet mauris, in hendrerit justo. Curabitur et tellus mauris. Duis nisi sem, rutrum quis libero eget, dictum lacinia sem. Integer consequat elementum sem, a laoreet turpis facilisis at. Phasellus et viverra enim, nec ornare massa. Pellentesque ut venenatis sem, ut rutrum urna. Aliquam faucibus fringilla lectus, at ornare leo vehicula id. Etiam sed nulla quam. Curabitur vitae quam euismod, luctus enim in, commodo nibh. Etiam ac eros vel felis ullamcorper aliquam. Cras at placerat mi.

Curabitur hendrerit ligula a mauris placerat, sed mattis eros molestie. Vivamus sit amet vestibulum mi, ac faucibus mi turpis duis.
Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed hendrerit, eros ac dictum euismod, turpis metus malesuada felis, vel placerat sapien ex id erat. Mauris ultrices, diam quis vehicula congue, massa nisi volutpat purus, et interdum ex leo ut odio. Maecenas scelerisque gravida massa nec semper. Quisque mollis dictum libero. Duis eu pretium ante. Sed et felis justo. Nam volutpat tincidunt mauris et faucibus.

Proin vitae nisl sit amet augue tincidunt aliquet. Vestibulum fermentum augue molestie, rhoncus tortor eget, finibus diam. Nulla posuere augue in porttitor lobortis. Integer sit amet convallis dolor. Ut fermentum dolor sapien, et feugiat eros porttitor nec. Duis tempus velit vitae ultrices dignissim. Cras egestas porttitor nisi in pulvinar. Vestibulum placerat metus at aliquam placerat. Interdum et malesuada fames ac ante ipsum primis in faucibus. Suspendisse auctor eleifend nisi, vitae rhoncus elit lobortis eu. Vivamus facilisis massa mauris, a placerat nisl tempor ac. Vivamus sagittis tincidunt risus sit amet viverra. Vestibulum consequat pretium posuere. Vivamus velit mi, iaculis at sapien non, malesuada cursus metus. Mauris vel tellus et nisi bibendum ullamcorper. Nulla consectetur mauris vitae molestie vulputate.

Aliquam erat volutpat. Curabitur vel elit vehicula, faucibus justo a, lobortis orci. Praesent at luctus quam. Nunc lectus magna, lobortis et egestas ut, viverra eu lorem. Proin eget aliquet mauris, in hendrerit justo. Curabitur et tellus mauris. Duis nisi sem, rutrum quis libero eget, dictum lacinia sem. Integer consequat elementum sem, a laoreet turpis facilisis at. Phasellus et viverra enim, nec ornare massa. Pellentesque ut venenatis sem, ut rutrum urna. Aliquam faucibus fringilla lectus, at ornare leo vehicula id. Etiam sed nulla quam. Curabitur vitae quam euismod, luctus enim in, commodo nibh. Etiam ac eros vel felis ullamcorper aliquam. Cras at placerat mi.

Curabitur hendrerit ligula a mauris placerat, sed mattis eros molestie. Vivamus sit amet vestibulum mi, ac faucibus mi turpis duis.
""";

  public void assertEqual(final ByteArrayOutputStream actual, final CharSequence expected) {
    final var actualString = new String(actual.toByteArray(), StandardCharsets.UTF_8);
    Assertions.assertEquals(expected.toString(), actualString);
  }

  private void doMultiTest(final String... texts) {
    final var fullText = Strings.toString("", texts);
    doTest(fullText, writer -> {
      for (final var text : texts) {
        text.chars()
          .forEach(c -> writer.append((char)c));
      }
    });
    doTest(fullText, writer -> {
      for (final var text : texts) {
        writer.append(text);
      }
    });
    doTest(fullText, writer -> {
      for (final var text : texts) {
        writer.append(text, 0, text.length());
      }
    });
    doTest(fullText, writer -> {
      for (final var text : texts) {
        writer.write(text);
      }
    });
    doTest(fullText, writer -> {
      for (final var text : texts) {
        writer.write(text, 0, text.length());
      }
    });
    doTest(fullText, writer -> {
      for (final var text : texts) {
        writer.write(text.toCharArray());
      }
    });
    doTest(fullText, writer -> {
      for (final var text : texts) {
        writer.write(text.toCharArray(), 0, text.length());
      }
    });
    doTest(fullText, writer -> {
      for (final var text : texts) {
        text.chars()
          .forEach(c -> writer.write(c));
      }
    });
  }

  public void doTest(final String expected, final Consumer<BufferedWriterEx> action) {
    final var bytes = new ByteArrayOutputStream();
    try (
      var writer = BufferedWriterEx.forStream(bytes, 1024);) {
      action.accept(writer);
    }
    assertEqual(bytes, expected);
  }

  @Test
  void escaped() {
    doMultiTest("hello");
    doMultiTest(this.longText.substring(0, 1023));
    doMultiTest(this.longText.substring(0, 1024));
    doMultiTest(this.longText.substring(0, 1024));
    doMultiTest(this.longText.substring(0, 2047));
    doMultiTest(this.longText.substring(0, 2048));
    doMultiTest(this.longText.substring(0, 2049));
    doMultiTest(this.longText.substring(0, 1023), this.longText.substring(0, 1));
    doMultiTest(this.longText.substring(0, 1023), this.longText.substring(0, 2));

  }

}
