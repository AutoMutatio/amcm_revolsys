/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.olingo.server.api.uri;

import java.util.List;

import org.apache.olingo.server.api.uri.queryoption.CountOption;
import org.apache.olingo.server.api.uri.queryoption.DeltaTokenOption;
import org.apache.olingo.server.api.uri.queryoption.ExpandOption;
import org.apache.olingo.server.api.uri.queryoption.FilterOption;
import org.apache.olingo.server.api.uri.queryoption.FormatOption;
import org.apache.olingo.server.api.uri.queryoption.OrderByOption;
import org.apache.olingo.server.api.uri.queryoption.SearchOption;
import org.apache.olingo.server.api.uri.queryoption.SelectOption;
import org.apache.olingo.server.api.uri.queryoption.SkipOption;
import org.apache.olingo.server.api.uri.queryoption.SkipTokenOption;
import org.apache.olingo.server.api.uri.queryoption.TopOption;

/**
 * Used for URI info kind {@link UriInfoKind#crossjoin} to describe URIs like
 * http://.../serviceroot/$crossjoin(...)
 */
public interface UriInfoCrossjoin {

  /**
   * @return Object containing information of the $count option
   */
  CountOption getCountOption();

  /**
   * @return Object containing information of the $deltatoken option
   */
  DeltaTokenOption getDeltaTokenOption();

  /**
   * @return List of entity set names
   */
  List<String> getEntitySetNames();

  /**
   * @return Object containing information of the $expand option
   */
  ExpandOption getExpandOption();

  /**
   * @return Object containing information of the $filter option
   */
  FilterOption getFilterOption();

  /**
   * @return Object containing information of the $format option
   */
  FormatOption getFormatOption();

  /**
   * @return Object containing information of the $orderby option
   */
  OrderByOption getOrderByOption();

  /**
   * @return Object containing information of the $search option
   */
  SearchOption getSearchOption();

  /**
   * @return Object containing information of the $select option
   */
  SelectOption getSelectOption();

  /**
   * @return Object containing information of the $skip option
   */
  SkipOption getSkipOption();

  /**
   * @return Object containing information of the $skiptoken option
   */
  SkipTokenOption getSkipTokenOption();

  /**
   * @return Object containing information of the $top option
   */
  TopOption getTopOption();
}
