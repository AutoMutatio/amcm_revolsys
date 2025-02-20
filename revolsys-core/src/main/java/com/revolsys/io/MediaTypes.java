package com.revolsys.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.revolsys.collection.map.Maps;
import com.revolsys.io.file.Paths;
import com.revolsys.logging.Logs;
import com.revolsys.util.Property;

public class MediaTypes {

  private static boolean initialized;

  private static Map<String, Set<String>> mediaTypesByFileExtension = new HashMap<>();

  private static Map<String, String> mediaTypeByFileExtension = new HashMap<>();

  private static Map<String, String> fileExtensionByMediaType = new HashMap<>();

  private static Map<String, Set<String>> fileExtensionsByMediaType = new HashMap<>();

  private static Set<String> mediaTypeWithMultipleExtensions = new HashSet<>();

  public static String extension(final String mediaType) {
    init();
    String extension = fileExtensionByMediaType.get(mediaType);
    if (extension == null) {
      extension = fileExtensionByMediaType.get(mediaType.toLowerCase());
      if (extension == null) {
        return "bin";
      }
    }
    return extension;
  }

  /**
   * If the file extension is a registered extension then return that. Otherwise look up the
   * file extension by content type. Both the extension and mediaType are converted to lower
   * case.
   *
   * @param extension
   * @param mediaType
   * @return
   */
  public static String extension(String extension, String mediaType) {
    init();
    if (Property.hasValue(extension)) {
      extension = extension.strip()
        .toLowerCase();
      if (mediaTypeByFileExtension.containsKey(extension)) {
        return extension;
      }
    }

    if (Property.hasValue(mediaType)) {
      mediaType = mediaType.toLowerCase();
      final String result = fileExtensionByMediaType.get(mediaType);
      if (Property.hasValue(result)) {
        return result;
      }
    }

    if (Property.hasValue(extension)) {
      return extension;
    } else {
      return "bin";
    }
  }

  public static Set<String> extensions(final String mediaType) {
    init();
    return fileExtensionsByMediaType.getOrDefault(mediaType, Collections.emptySet());
  }

  private static void init() {
    if (!initialized) {
      synchronized (MediaTypes.class) {
        if (!initialized) {
          initialized = true;
          try (
            final InputStream in = MediaTypes.class
              .getResourceAsStream("/com/revolsys/format/mediaTypes.tsv");
            Reader fileReader = new InputStreamReader(in);
            BufferedReader dataIn = new BufferedReader(fileReader);) {
            String line = dataIn.readLine();
            for (line = dataIn.readLine(); line != null; line = dataIn.readLine()) {
              final int tabIndex = line.indexOf('\t');
              final String fileExtension = line.substring(0, tabIndex)
                .toLowerCase()
                .strip()
                .intern();
              final String mediaType = line.substring(tabIndex + 1)
                .toLowerCase()
                .strip()
                .intern();
              if (Property.hasValuesAll(fileExtension, mediaType)) {
                if (!mediaTypeByFileExtension.containsKey(fileExtension)) {
                  mediaTypeByFileExtension.put(fileExtension, mediaType);
                }
                Maps.addToSet(mediaTypesByFileExtension, fileExtension, mediaType);
                if (!mediaTypeWithMultipleExtensions.contains(mediaType)) {
                  if (fileExtensionByMediaType.containsKey(mediaType)) {
                    mediaTypeWithMultipleExtensions.add(mediaType);
                    fileExtensionByMediaType.remove(mediaType);
                  } else {
                    fileExtensionByMediaType.put(mediaType, fileExtension);
                  }
                }
                Maps.addToSet(fileExtensionsByMediaType, mediaType, fileExtension);
              }
            }
          } catch (final IOException e) {
            Logs.error(MediaTypes.class, "Cannot read media types", e);
          }
        }
      }
    }
  }

  public static String mediaType(final Path file) {
    final String fileExtension = Paths.getFileNameExtension(file);
    return mediaType(fileExtension);
  }

  public static String mediaType(final String fileExtension) {
    init();
    if (fileExtension == null) {
      return "application/octet-stream";
    } else {
      String mediaType = mediaTypeByFileExtension.get(fileExtension);
      if (mediaType == null) {
        final String fileExtensionLower = fileExtension.toLowerCase();
        mediaType = mediaTypeByFileExtension.get(fileExtensionLower);
      }
      return mediaType;
    }
  }

  /**
   * If the file mediaType is a registered mediaType then return that. Otherwise look up the
   * file mediaType by extension. Both the extension and mediaType are converted to lower
   * case.
   *
   * @param extension
   * @param mediaType
   * @return
   */
  public static String mediaType(String mediaType, String extension) {
    init();
    if (Property.hasValue(mediaType)) {
      mediaType = mediaType.strip()
        .toLowerCase();
      if (fileExtensionByMediaType.containsKey(mediaType)) {
        return mediaType;
      }
    }

    if (Property.hasValue(extension)) {
      extension = extension.toLowerCase();
      final String result = mediaTypeByFileExtension.get(extension);
      if (Property.hasValue(result)) {
        return result;
      }
    }

    if (Property.hasValue(mediaType)) {
      return mediaType;
    } else {
      return "application/octet-stream";
    }
  }
}
