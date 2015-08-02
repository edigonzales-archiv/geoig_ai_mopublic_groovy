package org.catais.geoig.ai.mopublic

import groovy.util.logging.Log4j2
import groovy.sql.Sql
import java.sql.SQLException

@Log4j2
class MOpublicSQLFunctions {

	private def files = [
		'sequence.sql', 
		'metadata_metadata.sql', 
		'control_points_control_point.sql', 
		'land_cover_lcsurface.sql', 
		'land_cover_lcsurfaceproj.sql', 
		'local_names_names.sql',
		'ownership_boundary_point.sql', 
		'ownership_realestate.sql', 
		'ownership_dpr_mine.sql',
		'ownership_dpr_mineproj.sql', 
		'single_objects_surface_element.sql', 
		'single_objects_linear_element.sql',
		'single_objects_point_element.sql', 
		'territorial_boundaries_boundary_terr_point.sql',
		'territorial_boundaries_municipal_boundary.sql', 
		'territorial_boundaries_municipal_boundproj.sql',
		'territorial_boundaries_other_territ_boundary.sql', 
		'building_addresses.sql',
		'update_mopublic.sql'
		]
	
	void createFunctions(String dburl, String dbschema) {
		// Loop through all the files and concat the sql together to one query string.
		// Set search_path (aka schema).
		def query = "SET search_path TO ${Sql.expand(dbschema)};\n"
		files.each() { fileName ->
			
			URL url = this.class.classLoader.getResource("sql/${fileName}")
			query += url.text
		}
						
		// Create the functions.
		def sql = Sql.newInstance(dburl)
		sql.connection.autoCommit = false
					
		try {
			sql.execute(query)
			sql.commit()
		} catch (SQLException e) {
			sql.rollback()
			log.error e.getMessage()
			throw new SQLException(e)
		} finally {
			sql.connection.close()
			sql.close()
		}
	}
}
