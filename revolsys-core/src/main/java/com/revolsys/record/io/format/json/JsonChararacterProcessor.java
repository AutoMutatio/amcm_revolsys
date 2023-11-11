package com.revolsys.record.io.format.json;

import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.Deque;

import com.revolsys.reactive.chars.CharacterProcessor;

public class JsonChararacterProcessor implements CharacterProcessor {

  private class State {
    private final String name;

    private final StateProcessor processor;

    public State(final String name, final StateProcessor processor) {
      this.name = name;
      this.processor = processor;
    }

    @Override
    public String toString() {
      return this.name;
    }
  }

  private interface StateProcessor {
    void process(char c);
  }

  private static final String FALSE = "false";

  private static final String NULL = "null";

  private static final String TRUE = "true";

  private int constantIndex;

  private final JsonProcessor processor;

  private final Deque<State> states = new ArrayDeque<>(64);

  private final State stateArray = new State("ARRAY", this::processArray);

  private final State stateArrayNext = new State("ARRAY_NEXT", this::processArrayNext);

  private final State stateArrayValue = new State("ARRAY_VALUE", this::processArrayValue);

  private final State stateEscape = new State("ESCAPE", this::processEscape);

  private final State stateEscapeUnicode = new State("ESCAPE_UNICODE", this::processEscapeUnicode);

  private final State stateFalse = new State("FALSE", this::processFalse);

  private final State stateNull = new State("NULL", this::processNull);

  private final State stateNumber = new State("NUMBER", this::processNumber);

  private final State stateStartDocument = new State("START_DOCUMENT", this::processStartDocument);

  private final State stateString = new State("STRING", this::processString);

  private final State stateText = new State("TEXT", this::processText);

  private final State stateObjectColon = new State("OBJECT_COLON", this::processObjectColon);

  private final State stateObjectLabel = new State("OBJECT_LABEL", this::processObjectLabel);

  private final State stateObjectValue = new State("OBJECT_VALUE", this::processObjectValue);

  private final State stateObjectNext = new State("OBJECT_NEXT", this::processObjectNext);

  private final State stateObject = new State("OBJECT", this::processObject);

  private final State stateTrue = new State("TRUE", this::processTrue);

  private final StringBuilder text = new StringBuilder();

  private final StringBuilder unicode = new StringBuilder();

  private final JsonStatus status = new JsonStatus();

  public JsonChararacterProcessor(final JsonProcessor processor) {
    this.processor = processor;
  }

  private void doEndArray() {
    statePop();
    this.processor.endArray(this.status);
  }

  private void doEndObject() {
    statePop();
    this.processor.endObject(this.status);
  }

  private void doStartArray() {
    final JsonStateArray state = new JsonStateArray();
    statePush(this.stateArray, state);
    this.processor.startArray(this.status);
  }

  private void doStartObject() {
    final JsonStateObject state = new JsonStateObject();
    statePush(this.stateObject, state);
    this.processor.startObject(this.status);
  }

  @Override
  public void onCancel() {
    this.processor.onCancel();
  }

  @Override
  public void onComplete() {
    this.processor.endDocument(this.status);
  }

  @Override
  public boolean process(final char c) {
    State state;
    if (this.states.isEmpty()) {
      state = this.stateStartDocument;
      this.states.push(state);
      this.processor.startDocument(this.status);
    } else {
      state = this.states.peek();
    }
    state.processor.process(c);
    return true;
  }

  private void processArray(final char c) {
    if (c == ']') {
      doEndArray();
    } else {
      processArrayValue(c);
    }
  }

  private void processArrayNext(final char c) {
    if (Character.isWhitespace(c)) {
    } else if (c == ']') {
      doEndArray();
    } else if (c == ',') {
      replaceState(this.stateArrayValue);
    }
  }

  private void processArrayValue(final char c) {
    if (Character.isWhitespace(c)) {
    } else {
      replaceState(this.stateArrayNext);
      this.processor.beforeArrayValue(this.status);
      final JsonStateArray state = this.status.statePeek();
      state.increment();
      processNextChar(c);
    }
  }

  private void processEscape(final char c) {
    switch (c) {
      case 'b':
        this.text.append('\b');
      break;
      case '"':
        this.text.append('"');
      break;
      case '/':
        this.text.append('/');
      break;
      case '\\':
        this.text.append('\\');
      break;
      case 'f':
        this.text.append('\f');
      break;
      case 'n':
        this.text.append('\n');
      break;
      case 'r':
        this.text.append('\r');
      break;
      case 't':
        this.text.append('\t');
      break;
      case 'u':
        this.states.pop();
        this.states.push(this.stateEscapeUnicode);
        this.unicode.setLength(0);
        return;
      default:
        throw new IllegalStateException("Unexpected escape character: " + c);
    }
    this.states.pop();

  }

  private void processEscapeUnicode(final char c) {
    if ('0' <= c && c <= '9' || 'a' <= c && c <= 'f' || 'A' <= c && c <= 'F') {
      this.unicode.append(c);
      if (this.unicode.length() == 4) {
        try {
          final int unicode = Integer.parseInt(this.unicode, 0, 4, 16);
          this.text.append((char)unicode);
          this.states.pop();
        } catch (final NumberFormatException e) {
          throw e;
        }
      }
    } else {
      throw new IllegalStateException("Unicode escape must be a hex character " + c);
    }
  }

  private void processFalse(final char c) {
    if (FALSE.charAt(this.constantIndex++) == c) {
      if (FALSE.length() == this.constantIndex) {
        this.states.pop();
        this.processor.value(this.status, false);
      }
    } else {
      throw new IllegalStateException("Unexpected character: " + c);
    }
  }

  private void processNextChar(final char c) {
    switch (c) {
      case '{':
        doStartObject();
      break;
      case '[':
        doStartArray();
      break;
      case '"':
        this.states.push(this.stateString);
        this.states.push(this.stateText);
      break;
      case 'f':
        this.states.push(this.stateFalse);
        this.constantIndex = 1;
      break;
      case 'n':
        this.states.push(this.stateNull);
        this.constantIndex = 1;
      break;
      case 't':
        this.states.push(this.stateTrue);
        this.constantIndex = 1;
      break;
      default:
        if (c == '-' || '0' <= c && c <= '9') {
          this.text.setLength(0);
          this.text.append(c);
          this.states.push(this.stateNumber);
        } else if (Character.isWhitespace(c)) {
          // skip
        } else {
          throw new IllegalStateException("Unexpected character: " + c);
        }
      break;
    }
  }

  private void processNull(final char c) {
    if (NULL.charAt(this.constantIndex++) == c) {
      if (NULL.length() == this.constantIndex) {
        this.states.pop();
        this.processor.nullValue(this.status);
      }
    } else {
      throw new IllegalStateException("Unexpected character: " + c);
    }
  }

  private void processNumber(final char c) {
    boolean endArray = false;
    boolean endObject = false;
    if (c == '+') {
      final char lastChar = this.text.charAt(this.text.length() - 1);
      if (lastChar == 'e' || lastChar == 'E') {
        this.text.append(c);
      } else {
        throw new IllegalStateException("Numbers can't have the minus sign twice: " + c);
      }
    } else if (c == '-') {
      if (this.text.length() == 0) {
        this.text.append(c);
      } else {
        final char lastChar = this.text.charAt(this.text.length() - 1);
        if (lastChar == 'e' || lastChar == 'E') {
          this.text.append(c);
        } else {
          throw new IllegalStateException("Numbers can't have the minus sign twice: " + c);
        }
      }
    } else if (c == '.') {
      if (this.text.indexOf(".") == -1) {
        this.text.append(c);
      } else {
        throw new IllegalStateException("Numbers can't have the . twice: " + c);
      }
    } else if (c == 'e' || c == 'E') {
      if (this.text.indexOf("e") == -1 && this.text.indexOf("E") == -1) {
        this.text.append(c);
      } else {
        throw new IllegalStateException("Numbers can't have the e (exponent) twice: " + c);
      }
    } else if ('0' <= c && c <= '9') {
      this.text.append(c);
    } else {
      this.states.pop();
      if (c == '}') {
        endObject = true;
      } else if (c == ']') {
        endArray = true;
      } else if (c == ',') {
        if (this.states.peek() == this.stateObjectNext) {
          replaceState(this.stateObjectLabel);
        } else if (this.states.peek() == this.stateArrayNext) {
          replaceState(this.stateArrayValue);
        }
      }
      final String text = this.text.toString();
      final BigDecimal number = new BigDecimal(text);
      this.processor.value(this.status, number);
      this.text.setLength(0);
      if (endObject) {
        doEndObject();
      }
      if (endArray) {
        doEndArray();
      }
    }
  }

  private void processObject(final char c) {
    if (c == '}') {
      doEndObject();
    } else {
      processObjectLabel(c);
    }
  }

  private void processObjectColon(final char c) {
    if (Character.isWhitespace(c)) {
    } else if (c == ':') {
      final String label = this.text.toString();
      this.text.setLength(0);
      final JsonStateObject state = this.status.statePeek();
      state.setLabel(label);
      this.processor.label(this.status, label);
      replaceState(this.stateObjectNext);
      this.states.push(this.stateObjectValue);
    } else {
      throw new IllegalStateException("Expecting ':' not: '" + c + "'");
    }
  }

  private void processObjectLabel(final char c) {
    if (Character.isWhitespace(c)) {
    } else if (c == '"') {
      replaceState(this.stateObjectColon);
      this.states.push(this.stateText);
      this.text.setLength(0);
    } else {
      throw new IllegalStateException("Unexpected character: " + c);
    }
  }

  private void processObjectNext(final char c) {
    if (Character.isWhitespace(c)) {
    } else if (c == ',') {
      replaceState(this.stateObjectLabel);
    } else if (c == '}') {
      doEndObject();
    } else {
      throw new IllegalStateException("Expecting ',' or '}' not: '" + c + "'");
    }
  }

  private void processObjectValue(final char c) {
    if (Character.isWhitespace(c)) {
    } else {
      this.states.pop();
      processNextChar(c);
    }
  }

  private void processStartDocument(final char c) {
    processNextChar(c);
  }

  private void processString(final char c) {
  }

  private void processText(final char c) {
    if (c == '\\') {
      this.states.push(this.stateEscape);
    } else if (c == '"') {
      this.states.pop();
      if (this.states.peek() == this.stateString) {
        this.states.pop();
        this.processor.value(this.status, this.text.toString());
        this.text.setLength(0);
      }
    } else {
      this.text.append(c);
    }
  }

  private void processTrue(final char c) {
    if (TRUE.charAt(this.constantIndex++) == c) {
      if (TRUE.length() == this.constantIndex) {
        this.states.pop();
        this.processor.value(this.status, true);
      }
    } else {
      throw new IllegalStateException("Unexpected character: " + c);
    }

  }

  private void replaceState(final State state) {
    this.states.pop();
    this.states.push(state);
  }

  private void statePop() {
    this.states.pop();
    this.status.statePop();
  }

  private void statePush(final State s1, final JsonState s2) {
    this.states.push(s1);
    this.status.statePush(s2);
  }
}
