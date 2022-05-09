
/*
Copyright 2020 Curtin University

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Author: Richard Hosking
*/

{% macro aggregate_relations(aggregate, aggregate_ref, facet) %}
/*
The purpose of this script it to export a range of sections from any of the aggregration tables (country, institution, group, etc)
The sections this applies to are: institution, memember, country, funders, publishers and journals
Primarily, the goal is to create what is a nested array, and turn that into a flat table that can be exported into Elasticsearch for use with Kibana
*/

SELECT
  entity.id as {{ aggregate }}_id,
  entity.name as {{ aggregate }}_name,
  entity.country as {{ aggregate }}_country,
  entity.country_code as {{ aggregate }}_country_code,
  entity.region as {{ aggregate }}_region,
  entity.subregion as {{ aggregate }}_subregion,
  entity.coordinates as {{ aggregate }}_coordinates,
  DATE(entity.time_period, 12, 31) AS published_year,
  relation.id as {{ facet }}_id,
  relation.name as {{ facet }}_name,
  relation.country as {{ facet }}_country,
  relation.country_code as {{ facet }}_country_code,
  relation.region as {{ facet }}_region,
  relation.subregion as {{ facet }}_subregion,
  relation.coordinates as {{ facet }}_coordinates,
  relation.total_outputs as {{ facet }}_total_outputs,
  relation.num_oa_outputs as {{ facet }}_num_oa_outputs,
  relation.num_green_outputs as {{ facet }}_num_green_outputs,
  relation.num_gold_outputs as {{ facet }}_num_gold_outputs,
  relation.citations.mag as {{ facet }}_total_citations,
  ROUND(SAFE_DIVIDE( ( relation.num_oa_outputs ) * 100 , relation.total_outputs ), 2) as {{ facet }}_percent_oa,
  ROUND(SAFE_DIVIDE( ( relation.num_green_outputs ) * 100 , relation.total_outputs ), 2) as {{ facet }}_percent_green,
  ROUND(SAFE_DIVIDE( ( relation.num_gold_outputs ) * 100 , relation.total_outputs ), 2) as {{ facet }}_percent_gold,
  relation.percentage_of_all_outputs as {{ facet }}_percent_of_all_outputs,
  relation.percentage_of_all_oa as {{ facet }}_percent_of_all_oa
FROM
  {{ aggregate_ref }} as entity,
  UNNEST({{ facet }}) as relation
ORDER BY entity.id, published_year ASC
{% endmacro %}