# DOI Workflow

The primary purpose of the DOI workflow is to link together open datasets of higher education through the use of persistent identifiers, and to then produce useful summary statistics across a variety of common aggregations

## Dataflow Digram

Insert diagram from LucidCharts here

## Input datasets

Description of these phase...

```eval_rst
+-------------------+-----------------------------------------------------+-----------+
| Name              | Table                                               | PID       |
+===================+=====================================================+===========+
| Crossref Metadata | `crossref.crossref_metadataYYYYMMDD`                | DOI       |
+-------------------+-----------------------------------------------------+-----------+
| Crossref Events   | `crossref.crossref_events`                          | DOI       |
+-------------------+-----------------------------------------------------+-----------+
| Crosref Fundref   | `crossref.crossref_fundrefYYYYMMDD`                 | FunderDOI |
+-------------------+-----------------------------------------------------+-----------+
| Grid              | `digital_science.gridYYYYMMDD`                      | GRID      |
+-------------------+-----------------------------------------------------+-----------+
| MAG               | `mag.*YYYYMMDD` (multiple individual tables)         | GRID/DOI  |
+-------------------+-----------------------------------------------------+-----------+
| Open Citations    | `open_citations.open_citationsYYYYMMDD`             | DOI       |
+-------------------+-----------------------------------------------------+-----------+
| ORCID             | `orcid.orcid`                                       | ORCID/DOI |
+-------------------+-----------------------------------------------------+-----------+
| Unpaywall         | `our_research.unpaywall`                            | DOI       |
+-------------------+-----------------------------------------------------+-----------+
| iso3166           | `ios.iso3166_countries_and_regions`                 | Country   |
+-------------------+-----------------------------------------------------+-----------+
```

## Intermediate datasets

Description of these phase...

```eval_rst
+----------------+----------------------------------------------------+-----+-----------------------------------+
| Name           | Table                                              | PID | SQL File Name                     |
+================+====================================================+=====+===================================+
| Crossref Events| `observatory_intermediate.crossref_eventsYYYYMMDD` | DOI | create_crossref_events.sql.jinja2 |
+----------------+----------------------------------------------------+-----+-----------------------------------+
| Crosref Fundref| `observatory_intermediate.crossref_fundrefYYYYMMDD`| DOI | create_crossref_fundref.sql.jinja2|
+----------------+----------------------------------------------------+-----+-----------------------------------+
| Grid           | `observatory_intermediate.gridYYYYMMDD`            | GRID| create_grid.sql.jinja2            |
+----------------+----------------------------------------------------+-----+-----------------------------------+
| MAG            | `observatory_intermediate.magYYYYMMDD`             | DOI | create_mag.sql.jinja2             |
+----------------+----------------------------------------------------+-----+-----------------------------------+
| Open Citations | `observatory_intermediate.open_citationsYYYYMMDD`  | DOI | create_open_citations.sql.jinja2  |
+----------------+----------------------------------------------------+-----+-----------------------------------+
| ORCID          | `observatory_intermediate.orcidYYYYMMDD`           | DOI | create_orcid.sql.jinja2           |
+----------------+----------------------------------------------------+-----+-----------------------------------+
| Unpaywall      | `observatory_intermediate.unpaywallYYYYMMDD`       | DOI | create_unpaywall.sql.jinja2       |
+----------------+----------------------------------------------------+-----+-----------------------------------+

```

## Output datasets

Description of these phase...

```eval_rst
+-----------------+---------------------------------------------+-----------+---------------------------------+
| Name            | Table                                       | PID       | SQL File Name                   |
+=================+=============================================+===========+=================================+
| DOI             | `observatory.doiYYYYMMDD`                   | DOI       | create_doi.sql.jinja2           |
+-----------------+---------------------------------------------+-----------+---------------------------------+
| Author          | `observatory.authorYYYYMMDD`                | ORCID     | create_aggregate.sql.jinja2     |
+-----------------+---------------------------------------------+-----------+---------------------------------+
| Book            | `observatory.bookYYYYMMDD`                  | ISBN      | create_book.sql.jinja2          |
+-----------------+---------------------------------------------+-----------+---------------------------------+
| Country         | `observatory.countryYYYYMMDD`               | Alpha3    | create_aggregate.sql.jinja2     |
+-----------------+---------------------------------------------+-----------+---------------------------------+
| Funder          | `observatory.funderYYYYMMDD`                | Name      | create_aggregate.sql.jinja2     |
+-----------------+---------------------------------------------+-----------+---------------------------------+
| Group           | `observatory.groupYYYYMMDD`                 | Name      | create_aggregate.sql.jinja2     |
+-----------------+---------------------------------------------+-----------+---------------------------------+
| Institution     | `observatory.institutionYYYYMMDD`           | GRID      | create_aggregate.sql.jinja2     |
+-----------------+---------------------------------------------+-----------+---------------------------------+
| Journal         | `observatory.journalYYYYMMDD`               | ISSN-l    | create_aggregate.sql.jinja2     |
+-----------------+---------------------------------------------+-----------+---------------------------------+
| Publisher       | `observatory.publisherYYYYMMDD`             | Name      | create_aggregate.sql.jinja2     |
+-----------------+---------------------------------------------+-----------+---------------------------------+
| Region          | `observatory.regionYYYYMMDD`                | Region    | create_aggregate.sql.jinja2     |
+-----------------+---------------------------------------------+-----------+---------------------------------+
| Sub Region      | `observatory.subregionYYYYMMDD`             | SubRegion | create_aggregate.sql.jinja2     |
+-----------------+---------------------------------------------+-----------+---------------------------------+
```

## Testing

Overview of how the tests work here...