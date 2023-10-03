SELECT
  *
FROM (
  -- query 1 - find all the flooding areas
  SELECT
    geo AS flood_poly,
    ST_AREA(geo) AS area
  FROM
    `your-project.your_dataset.your_table`
  WHERE
    ST_AREA(geo) < 500000 ) t1 -- eliminate admin areas in the dataset
JOIN (
  -- query 2 - find all the highways in Open Street Map - https://wiki.openstreetmap.org/wiki/BigQuery_dataset#Query_2:_hospitals_with_no_phone_tag
  SELECT
    id,
    version,
    changeset,
    osm_timestamp,
    geometry as road_geometry
  FROM
    `bigquery-public-data.geo_openstreetmap.planet_ways` planet_ways,
    planet_ways.all_tags AS all_tags
  WHERE
  -- this tag catches all types of roads https://wiki.openstreetmap.org/wiki/Map_features
    all_tags.key = 'highway' ) 
ON
  ST_INTERSECTS(flood_poly, road_geometry)