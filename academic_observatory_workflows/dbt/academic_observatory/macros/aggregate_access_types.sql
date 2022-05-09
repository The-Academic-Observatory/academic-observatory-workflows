
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

Author: James Diprose, Richard Hosking
*/

{% macro aggregate_access_types(aggregate, aggregate_ref) %}
/*
The purpose of this script it to export the access_types section from any of the aggregration tables (country, institution, group, etc)
Primarily, the goal is to create what is a nested array, and turn that into a flat table that can be exported into Elasticsearch for use with Kibana
*/
    SELECT
    id as {{ aggregate }}_id,
    name as {{ aggregate }}_name,
    country as {{ aggregate }}_country,
    country_code as {{ aggregate }}_country_code,
    region as {{ aggregate }}_region,
    subregion as {{ aggregate }}_subregion,
    coordinates as {{ aggregate }}_coordinates,
    DATE(time_period, 12, 31) AS published_year,
    access_type.access_type as access_types_access_type,
    access_type.status as access_types_status,
    access_type.label as access_types_label,
    access_type.total_outputs as access_types_total_outputs,
    access_type.outputs_with_citations as access_types_outputs_with_citations,
    access_type.outputs_without_citations as access_types_outputs_without_citations,
    access_type.citations.mag.total_citations as access_types_total_citations,
    access_type.citations.mag.percentiles.median as access_types_median_citations_per_output
    FROM {{ aggregate_ref }}, UNNEST( access_types.breakdown ) as access_type
    ORDER BY id, published_year ASC

{% endmacro %}