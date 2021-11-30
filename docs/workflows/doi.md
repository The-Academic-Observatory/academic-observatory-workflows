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
| Grid              |                                                     | GRID      |
+-------------------+-----------------------------------------------------+-----------+
| MAG               |                                                     | GRID/DOI  |
+-------------------+-----------------------------------------------------+-----------+
| Open Citations    |                                                     | DOI       |
+-------------------+-----------------------------------------------------+-----------+
| ORCID             |                                                     | ORCID/DOI |
+-------------------+-----------------------------------------------------+-----------+
| Unpaywall         |                                                     | DOI       |
+-------------------+-----------------------------------------------------+-----------+
| iso3166           |                                                     | Country   |
+-------------------+-----------------------------------------------------+-----------+
```

## Intermedia datasets

Description of these phase...

```eval_rst
+-----------------+-----------------------------------------------------+-------+-----------------------------------+
| Name            | Table                                               | PID   | SQL File Name                     |
+=================+=====================================================+=======+===================================+
| Crossref Events | `observatory_intermediate.crossref_eventsYYYYMMDD`  | DOI   | create_crossref_events.sql.jinja2 |
+-----------------+-----------------------------------------------------+-------+-----------------------------------+
| Crosref Fundref | `observatory_intermediate.crossref_fundrefYYYYMMDD` | DOI   | create_crossref_fundref.sql.jinja2|
+-----------------+-----------------------------------------------------+-------+-----------------------------------+
| Grid            | `observatory_intermediate.gridYYYYMMDD`             | GRID  | create_grid.sql.jinja2            |
+-----------------+-----------------------------------------------------+-------+-----------------------------------+
| MAG             | `observatory_intermediate.magYYYYMMDD`              | DOI   | create_mag.sql.jinja2             |
+-----------------+-----------------------------------------------------+-------+-----------------------------------+
| Open Citations  | `observatory_intermediate.open_citationsYYYYMMDD`   | DOI   | create_open_citations.sql.jinja2  |
+-----------------+-----------------------------------------------------+-------+-----------------------------------+
| ORCID           | `observatory_intermediate.orcidYYYYMMDD`            | DOI   | create_orcid.sql.jinja2           |
+-----------------+-----------------------------------------------------+-------+-----------------------------------+
| Unpaywall       | `observatory_intermediate.unpaywallYYYYMMDD`        | DOI   | create_unpaywall.sql.jinja2       |
+-----------------+-----------------------------------------------------+-------+-----------------------------------+

```

## Output datasets

Description of these phase...

```eval_rst
+-----------------+-----------------------------------------------------+-------+-----------------------------------+
| Name            | Table                                               | PID   | SQL File Name                     |
+=================+=====================================================+=======+===================================+
|                 |                                                     |       |                                   |
+-----------------+-----------------------------------------------------+-------+-----------------------------------+
```

## Testing

Overview of how the tests work here...