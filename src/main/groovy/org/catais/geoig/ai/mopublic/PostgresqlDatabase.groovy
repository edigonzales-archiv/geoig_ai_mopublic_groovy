package org.catais.geoig.ai.mopublic

import groovy.util.logging.Log4j2
import groovy.sql.Sql
import java.sql.SQLException

import ch.ehi.ili2db.base.Ili2db
import ch.ehi.ili2db.base.Ili2dbException
import ch.ehi.ili2db.gui.Config
import ch.ehi.ili2pg.converter.PostgisGeometryConverter
import ch.ehi.sqlgen.generator_impl.jdbc.GeneratorPostgresql

@Log4j2
class PostgresqlDatabase {
	
	String dbhost = "localhost"
	String dbport = "5432"
	String dbdatabase = "xanadu2"
	String dbusr = "stefan"
	String dbpwd = "ziegler12"
	String dbschema = "av_mopublic_export"
	String dburl = "jdbc:postgresql://${dbhost}:${dbport}/${dbdatabase}"

	String modelName = "MOpublic03_ili2_v13"
	
//	PostgresqlDatabase(Map options = [:]) {
//		options.each { entry ->
//		    println "Name: $entry.key Age: $entry.value"
//		}		
//	}
	
	void initSchema() {
		initSchema(dbschema)
	}
	
	void initSchema(String dbschema) {
		this.dbschema = dbschema
		
		// 0) Drop cascade schema if exists.
		def sql = Sql.newInstance(dburl)
		sql.connection.autoCommit = false
							
		try {
			sql.execute("DROP SCHEMA IF EXISTS ${Sql.expand(dbschema)} CASCADE;")	
			sql.commit()
		} catch (SQLException e) {
			sql.rollback()
			log.error e.getMessage()
			throw new SQLException(e)
		} finally {
			sql.connection.close()
			sql.close()
		}
				
		// 1) Create schema and tables with ili2pg.
		def config = ili2dbConfig()
		Ili2db.runSchemaImport(config, "");
		
		// 2) Insert functions into schema.
		// These functions are used to for "Datenumbau"
		// from DM01AVCH24D -> MOpublic03_ili2_v13
		def functionFiles = ['sequence.sql', 'metadata_metadata.sql', 'control_points_control_point.sql', 
			'land_cover_lcsurface.sql', 'land_cover_lcsurfaceproj.sql', 'local_names_names.sql',
			'ownership_boundary_point.sql', 'ownership_realestate.sql', 'ownership_dpr_mine.sql',
			'ownership_dpr_mineproj.sql', 'single_objects_surface_element.sql', 'single_objects_linear_element.sql',
			'single_objects_point_element.sql', 'territorial_boundaries_boundary_terr_point.sql',
			'territorial_boundaries_municipal_boundary.sql', 'territorial_boundaries_municipal_boundproj.sql',
			'territorial_boundaries_other_territ_boundary.sql']
		
		// Loop through all the files and concat the sql together to one query string.
		// Set search_path (aka schema).
		def query = "SET search_path TO ${Sql.expand(dbschema)};\n"
		functionFiles.each() { fileName ->
			query += sqlFunctionFromFile(fileName, dbschema)
		}
		
		// Create the functions.		
		sql = Sql.newInstance(dburl)
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
	
	private def sqlFunctionFromFile(fileName, dbschema) {
		log.debug(fileName)
		URL url = this.class.classLoader.getResource("sql/${fileName}")
		return url.text
	}
	
	private def ili2dbConfig() {
		def config = new Config()
		config.setDbhost(dbhost)
		config.setDbdatabase(dbdatabase)
		config.setDbport(dbport)
		config.setDbusr(dbusr)
		config.setDbpwd(dbpwd)
		config.setDbschema(dbschema)
		config.setDburl(dburl)
		
		config.setModels(modelName);
		config.setModeldir("http://models.geo.admin.ch/");
		
		config.setGeometryConverter(PostgisGeometryConverter.class.getName())
		config.setDdlGenerator(GeneratorPostgresql.class.getName())
		config.setJdbcDriver("org.postgresql.Driver")

		config.setNameOptimization("topic")
		config.setMaxSqlNameLength("60")
		config.setStrokeArcs("enable")
		config.setSqlNull("enable"); // be less restrictive
		config.setValue("ch.ehi.sqlgen.createGeomIndex", "True");
		
		config.setDefaultSrsAuthority("EPSG")
		config.setDefaultSrsCode("21781")
		
		return config
	}
}
