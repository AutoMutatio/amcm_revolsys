package org.apache.olingo.commons.api.data;

import java.net.URI;
import java.util.Collections;
import java.util.List;

import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;

public interface ODataEntity extends ODataLinked, ODataPropertyMap {
  public static ODataEntity EMPTY = new ODataEntity() {};

  default List<Annotation> getAnnotations() {
    return Collections.emptyList();
  }

  /**
   * Gets entity edit link.
   *
   * @return edit link.
   */
  default Link getEditLink() {
    return null;
  }

  /**
   * Gets ETag.
   *
   * @return ETag.
   */
  default String getETag() {
    return null;
  }

  default ListEx<String> getFieldNames() {
    return Lists.empty();
  }

  /**
   * Gets media content resource.
   *
   * @return media content resource.
   */
  default URI getMediaContentSource() {
    return null;
  }

  /**
   * Gets media content type.
   *
   * @return media content type.
   */
  default String getMediaContentType() {
    return null;
  }

  /**
   * Gets media entity links.
   *
   * @return links.
   */
  default List<Link> getMediaEditLinks() {
    return Collections.emptyList();
  }

  /**
   * ETag of the binary stream represented by this media entity or named stream property.
   *
   * @return media ETag value
   */
  default String getMediaETag() {
    return null;
  }

  /**
   * Gets operations.
   *
   * @return operations.
   */
  default List<Operation> getOperations() {
    return Collections.emptyList();
  }

  /**
   * Gets property with given name.
   *
   * @param name property name
   * @return property with given name if found, null otherwise
   */
  @Override
  default Property getProperty(final String name) {
    return null;
  }

  /**
   * Gets entity self link.
   *
   * @return self link.
   */
  default Link getSelfLink() {
    return null;
  }

  /**
   * Gets entity type.
   *
   * @return entity type.
   */
  default String getType() {
    return null;
  }

  /**
   * Checks if the current entity is a media entity.
   *
   * @return 'TRUE' if is a media entity; 'FALSE' otherwise.
   */
  default boolean isMediaEntity() {
    return false;
  }

}
