package org.apache.olingo.commons.api.data;

import java.util.Collections;
import java.util.List;

public interface ODataLinked extends ODataObject {

  /**
   * Gets association link with given name, if available, otherwise <tt>null</tt>.
   *
   * @param name candidate link name
   * @return association link with given name, if available, otherwise <tt>null</tt>
   */
  default Link getAssociationLink(final String name) {
    return null;
  }

  /**
   * Gets association links.
   *
   * @return association links.
   */
  default List<Link> getAssociationLinks() {
    return Collections.emptyList();
  }

  /**
   * Gets binding link with given name, if available, otherwise <tt>null</tt>.
   * @param name candidate link name
   * @return binding link with given name, if available, otherwise <tt>null</tt>
   */
  default Link getNavigationBinding(final String name) {
    return null;
  }

  /**
   * Gets binding links.
   *
   * @return links.
   */
  default List<Link> getNavigationBindings() {
    return Collections.emptyList();
  }

  /**
   * Gets navigation link with given name, if available, otherwise <tt>null</tt>.
   *
   * @param name candidate link name
   * @return navigation link with given name, if available, otherwise <tt>null</tt>
   */
  default Link getNavigationLink(final String name) {
    return null;
  }

  /**
   * Gets navigation links.
   *
   * @return links.
   */
  default List<Link> getNavigationLinks() {
    return Collections.emptyList();
  }

}
