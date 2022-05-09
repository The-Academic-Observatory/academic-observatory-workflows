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

Author: Richard Hosking, James Diprose 
*/

SELECT
    UPPER(TRIM(crossref.doi)) as doi,
    ARRAY_AGG(STRUCT(funder, fundref)) as funders
FROM {{ source('crossref', 'crossref_metadata' ~ var('shard_date')) }} as crossref, UNNEST(crossref.funder) as funder
LEFT JOIN {{ source('crossref', 'crossref_fundref' ~ var('shard_date')) }} as fundref on SUBSTR(fundref.funder, 19) = funder.doi
GROUP BY UPPER(TRIM(crossref.doi))