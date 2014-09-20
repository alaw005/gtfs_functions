/*
Contents:
	Functions: 
		my_gtfs_generate_table_my_gtfs_trip_stats()
		my_gtfs_generate_table_my_gtfs_route_catchment()
		my_gtfs_generate_table_my_gtfs_route_trip_stop_sequence()
		
	Views: 
		my_gtfs_route_trip_stats 			-- links my_gtfs_trip_stats to other table data
		
	Tables created:
		my_gtfs_trip_stats					-- NB: use view my_gtfs_route_trip_stats
		my_gtfs_route_stop_catchment
		my_gtfs_route_catchment
		my_gtfs_route_path
		my_gtfs_route_trip_stop_sequence
		
Description:
	This sql creates and executes functions to create the tables specified above. It also defines the views set out above.
	
Dependencies:
	- gtfs data must have been imported into database
	- my_gtfs_generate_table_my_gtfs_route_catchment() requires stop catchments to have already been generated
	using my_gtfs_generate_table_my_gtfs_route_catchment() function.  

	
Usage:
	
Author:
	Adam Lawrence <alaw005@gmail.com>	
*/

/*
Name:
	my_gtfs_generate_table_my_gtfs_trip_stats - Creates table "my_gtfs_trip_stats" with trip statistics 
	including departure and arrival times, distance and in-service time 

Description:
	Generates table with trip stats as too slow running dynamically. Also refer View my_gtfs_route_trip_stats

Usage:
	SELECT my_gtfs_generate_table_my_gtfs_trip_stats();
	SELECT * FROM my_gtfs_trip_stats;
	SELECT * FROM my_gtfs_route_trip_stats;

Author:
	Adam Lawrence <alaw005@gmail.com>	
*/
DROP FUNCTION IF EXISTS my_gtfs_generate_table_my_gtfs_trip_stats();
CREATE OR REPLACE FUNCTION my_gtfs_generate_table_my_gtfs_trip_stats()
  RETURNS text AS
$BODY$
DECLARE
	-- Nothing to declare
BEGIN

	-- Create table to hold trip stats
	DROP TABLE IF EXISTS my_gtfs_trip_stats CASCADE;
	CREATE TABLE my_gtfs_trip_stats (
		trip_id text PRIMARY KEY,
		direction_id integer,
		depart_time text,
		arrive_time text,
		first_stop_id text,
		last_stop_id text,
		trip_distance_km float,
		trip_time_mins integer
	);

	-- Insert stats from the gtfs_stop_times table. Note that shape_dist_traveled is cumulative, also will need
	-- to separately calculate trip time  
	INSERT INTO my_gtfs_trip_stats (trip_id, direction_id, depart_time,arrive_time,first_stop_id,last_stop_id,trip_distance_km) SELECT 
			a.trip_id,
			a.direction_id,
			(SELECT departure_time FROM gtfs_stop_times AS b WHERE a.trip_id = b.trip_id ORDER BY stop_sequence LIMIT 1) AS depart_time,
			(SELECT departure_time FROM gtfs_stop_times AS b WHERE a.trip_id=b.trip_id ORDER BY stop_sequence DESC LIMIT 1) AS arrive_time,
			(SELECT stop_id FROM gtfs_stop_times AS b WHERE a.trip_id=b.trip_id ORDER BY stop_sequence LIMIT 1) AS first_stop_id,
			(SELECT stop_id FROM gtfs_stop_times AS b WHERE a.trip_id=b.trip_id ORDER BY stop_sequence DESC LIMIT 1) AS last_stop_id,
			-- Need to select approach to getting distance travelled, note may need to change /1000 or /100 depending on units
			-- Distance based on gtfs_shapes table (noting optional here)
			--(SELECT shape_dist_traveled FROM gtfs_shapes AS b WHERE a.shape_id=b.shape_id ORDER BY shape_pt_sequence DESC LIMIT 1)/1000::float AS trip_distance_km
			-- Or distance based on gtfs_stop_times table (noting optional here too)
			(SELECT shape_dist_traveled FROM gtfs_stop_times AS b WHERE a.trip_id=b.trip_id ORDER BY stop_sequence DESC LIMIT 1)/100::float AS trip_distance_km
		FROM gtfs_trips AS a;
	
		UPDATE my_gtfs_trip_stats AS a SET trip_time_mins = 
			((SELECT arrival_time_seconds FROM gtfs_stop_times AS b WHERE a.trip_id=b.trip_id ORDER BY stop_sequence DESC LIMIT 1) - 
				(SELECT departure_time_seconds FROM gtfs_stop_times AS c WHERE a.trip_id=c.trip_id ORDER BY stop_sequence LIMIT 1))/60;
				
	-- Create view for route trip stats
	CREATE OR REPLACE VIEW my_gtfs_route_trip_stats AS 
		SELECT DISTINCT
			gtfs_routes.route_short_name, 
			gtfs_routes.route_long_name, 
			CASE 
				WHEN CONCAT(monday,tuesday,wednesday,thursday,friday)::integer <> 0 THEN 'MF'
				WHEN saturday = 1 THEN 'Sat'
				WHEN sunday = 1 THEN 'Sun'
				ELSE NULL
			END AS Weekday,
			my_gtfs_trip_stats.direction_id::integer,
			my_gtfs_trip_stats.depart_time, 
			my_gtfs_trip_stats.arrive_time,
			gtfs_trips.trip_id, 
			my_gtfs_trip_stats.trip_distance_km, 
			my_gtfs_trip_stats.trip_time_mins::integer, 
			my_gtfs_trip_stats.first_stop_id,
			my_gtfs_trip_stats.last_stop_id,
		CONCAT(monday,tuesday,wednesday,thursday,friday)::text AS MF,
			saturday::text as Sat,
			sunday::text as Sun,
			gtfs_routes.route_id,
			gtfs_trips.shape_id,
			gtfs_calendar.service_id,
			gtfs_calendar.start_date,
			gtfs_calendar.end_date
		FROM gtfs_routes 
			LEFT JOIN gtfs_trips ON gtfs_trips.route_id = gtfs_routes.route_id
			LEFT JOIN my_gtfs_trip_stats ON my_gtfs_trip_stats.trip_id = gtfs_trips.trip_id
			LEFT JOIN gtfs_calendar ON gtfs_calendar.service_id = gtfs_trips.service_id
		ORDER BY 
			gtfs_routes.route_short_name, 
			CASE 
				WHEN CONCAT(monday,tuesday,wednesday,thursday,friday)::integer <> 0 THEN 'MF'
				WHEN saturday = 1 THEN 'Sat'
				WHEN sunday = 1 THEN 'Sun'
				ELSE NULL
			END,
			my_gtfs_trip_stats.direction_id, 
			my_gtfs_trip_stats.depart_time;
	  
				
	RETURN 'OK';  
	
END;
$BODY$
  LANGUAGE plpgsql VOLATILE;


/*
Name:
	my_gtfs_generate_table_my_gtfs_route_catchment - Creates table "my_gtfs_route_stop_catchment"  

Description:
	Generates table with route, stop network buffer. Generated as new table for performance reasons
	NB: Requires stop network buffers to have been generated previously
	
Usage:
	SELECT my_gtfs_generate_table_my_gtfs_route_catchment('SELECT id, source_id AS stop_id, geom AS the_geom FROM hutt_pax.tmp_gtfs_stops_400m_buffer');
	SELECT * FROM my_gtfs_route_stop_catchment;
	SELECT * FROM my_gtfs_route_catchment;

Author:
	Adam Lawrence <alaw005@gmail.com>	
*/
DROP FUNCTION IF EXISTS my_gtfs_generate_table_my_gtfs_route_catchment(text);
CREATE OR REPLACE FUNCTION my_gtfs_generate_table_my_gtfs_route_catchment(stopCatchmentSql text DEFAULT 'SELECT id, source_id AS stop_id, the_geom FROM hutt_pax.tmp_gtfs_stops_400m_buffer')
  RETURNS text AS
$BODY$
DECLARE
	-- Nothing to declare
BEGIN

	-- Create temp view with "stopCatchmentSql" sql, take this approach so don't have to use EXECUTE in main query
	EXECUTE 'CREATE OR REPLACE TEMP VIEW tmp_view_stop_catchment AS ' || stopCatchmentSql;

	-- Create table for route stop catchment
	DROP TABLE IF EXISTS my_gtfs_route_stop_catchment;
	CREATE TABLE my_gtfs_route_stop_catchment (
		id serial PRIMARY KEY,
		route_id text,
		route_name text,
		stop_id text,
		stop_name text,
		the_geom geometry
	);
	-- Run insert query
	INSERT INTO my_gtfs_route_stop_catchment (route_id, route_name, stop_id, stop_name, the_geom) 
		SELECT DISTINCT
				gtfs_routes.route_id,
				gtfs_routes.route_short_name AS route_name,
				gtfs_stop_times.stop_id,
				gtfs_stops.stop_name,
				tmp_view_stop_catchment.the_geom
			FROM gtfs_routes
				JOIN gtfs_trips ON gtfs_trips.route_id = gtfs_routes.route_id
				JOIN gtfs_stop_times ON gtfs_stop_times.trip_id = gtfs_trips.trip_id
				JOIN gtfs_stops ON gtfs_stops.stop_id = gtfs_stop_times.stop_id
				JOIN tmp_view_stop_catchment ON tmp_view_stop_catchment.stop_id::text = gtfs_stop_times.stop_id::text;

	-- Create table for aggregated route catchment
	DROP TABLE IF EXISTS my_gtfs_route_catchment;
	CREATE TABLE my_gtfs_route_catchment (
		id serial PRIMARY KEY,
		route_name text,
		the_geom geometry
	);
	-- Run insert query
	INSERT INTO my_gtfs_route_catchment (route_name, the_geom) 
		SELECT 
			route_name, 
			ST_Union(the_geom) AS the_geom
		FROM my_gtfs_route_stop_catchment
		GROUP BY route_name
		ORDER BY RIGHT(CONCAT('___',route_name),3);


	-- Create table for aggregated route path
	DROP TABLE IF EXISTS my_gtfs_route_path;
	CREATE TABLE my_gtfs_route_path (
		id serial PRIMARY KEY,
		route_name text,
		route_id text,
		shape_id text
	);
	-- Add geometry column (add separately so properly registered in the geometry_columns table)
	PERFORM  AddGeometryColumn ('public','my_gtfs_route_path','the_geom',4326,'MULTILINESTRING',2);	

	-- Run insert query approx 17000 ms
	INSERT INTO my_gtfs_route_path (route_name, route_id, shape_id, the_geom) 
		SELECT
			gtfs_routes.route_short_name AS route_name, 
			gtfs_routes.route_id,
			gtfs_shape_geoms.shape_id, 
			ST_Collect(gtfs_shape_geoms.the_geom) AS the_geom
		FROM gtfs_routes
			JOIN gtfs_trips ON gtfs_trips.route_id = gtfs_routes.route_id
			JOIN gtfs_shape_geoms ON gtfs_shape_geoms.shape_id = gtfs_trips.shape_id
		GROUP BY
			gtfs_routes.route_short_name, 
			gtfs_routes.route_id,
			gtfs_shape_geoms.shape_id
		ORDER BY 
			RIGHT(CONCAT('___',gtfs_routes.route_short_name),3);

	
	RETURN 'OK';
	
END;
$BODY$
  LANGUAGE plpgsql VOLATILE;

/*
Name:
	my_gtfs_generate_table_my_gtfs_route_trip_stop_sequence 

Notes:
	Need to run function "my_gtfs_generate_table_my_gtfs_trip_stats" first 
	
Dependencies:
	VIEW my_gtfs_route_trip_stats
Usage:
	SELECT my_gtfs_generate_table_my_gtfs_route_trip_stop_sequence();
	SELECT * FROM my_gtfs_route_trip_stop_sequence;
	
Author:
	Adam Lawrence <alaw005@gmail.com>	
*/
DROP FUNCTION IF EXISTS my_gtfs_generate_table_my_gtfs_route_trip_stop_sequence();
CREATE OR REPLACE FUNCTION my_gtfs_generate_table_my_gtfs_route_trip_stop_sequence()
  RETURNS text AS
$BODY$
DECLARE
	-- Nothing to declare
BEGIN

	-- Create table for route stop catchment
	DROP TABLE IF EXISTS my_gtfs_route_trip_stop_sequence;
	CREATE TABLE my_gtfs_route_trip_stop_sequence (
		id serial PRIMARY KEY,
		route_id text,
		route_name text,
		weekday text,
		direction_id integer,
		trip_id text,
		depart_time text,
		stop_sequence integer,
		departure_time text,
		departure_time_seconds integer,
		stop_id text,
		stop_name text,
		the_geom geometry
	);
	-- Run insert query
	INSERT INTO my_gtfs_route_trip_stop_sequence (route_id, route_name, weekday, direction_id, trip_id, depart_time, stop_sequence, departure_time, departure_time_seconds, stop_id, stop_name, the_geom) 
		SELECT
			a.route_id,
			a.route_short_name AS route_name,
			a.weekday,
			a.direction_id,
			a.trip_id,
			a.depart_time,
			b.stop_sequence,
			b.departure_time,
			b.departure_time_seconds,
			b.stop_id,
			c.stop_name,
			c.the_geom
		FROM my_gtfs_route_trip_stats AS a
			JOIN gtfs_stop_times AS b ON b.trip_id = a.trip_id
			JOIN gtfs_stops AS c ON c.stop_id = b.stop_id
		ORDER BY
			a.route_short_name,
			a.weekday,
			a.direction_id,
			a.trip_id,
			a.depart_time,
			b.stop_sequence;

	CREATE OR REPLACE VIEW my_gtfs_route_shape_stop_sequence AS
		SELECT DISTINCT
			a.route_name,
			a.weekday,
			a.direction_id,
			b.shape_id,
			a.stop_sequence,
			a.stop_id,
			a.stop_name
		FROM my_gtfs_route_trip_stop_sequence AS a
			JOIN gtfs_trips AS b ON a.trip_id = b.trip_id
		ORDER BY route_name, weekday, direction_id, shape_id, stop_sequence;
			
	RETURN 'OK';
	
END;
$BODY$
  LANGUAGE plpgsql VOLATILE;
	

/* 
	Run functions that generate required tables
*/
SELECT my_gtfs_generate_table_my_gtfs_trip_stats();
SELECT my_gtfs_generate_table_my_gtfs_route_trip_stop_sequence();

-- The following will only work if dependency table exists
--SELECT my_gtfs_generate_table_my_gtfs_route_catchment('SELECT id, source_id AS stop_id, the_geom FROM hutt_pax.tmp_gtfs_stops_400m_buffer');


	
