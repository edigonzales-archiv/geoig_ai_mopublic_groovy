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
	
	String exportDirectory = "/tmp/"
	String exportFormat = "gml"
	String fosnrQuery = "SELECT bfsnr FROM av_avdpool_ng.gemeindegrenzen_gemeinde;"
		
	void initSchema() {
		initSchema(dbschema)
	}
	
	void initSchema(String dbschema) {
		this.dbschema = dbschema
		
		// 0) Drop cascade schema if exists.
		dropSchema()
		log.debug "Existing schema dropped: ${this.dbschema}."
				
		// 1) Create schema and tables with ili2pg.
		def config = ili2dbConfig()
		Ili2db.runSchemaImport(config, "");
		
		// 2) Insert functions into schema.
		// These functions are used to for "Datenumbau"
		// from DM01AVCH24D -> MOpublic03_ili2_v13
		try {
			def functions = new MOpublicSQLFunctions()
			functions.createFunctions(dburl, dbschema)
		} catch (NullPointerException e) {
			// NPE is thrown when sql file is not found. In this case we need to
			// get rid of the schema we created with ili2db.
			log.error e.getMessage()
			dropSchema()
			log.debug "Schema dropped: ${this.dbschema}."
			throw new Exception(e)
		}
	}
	
	void runExport(String fosnr, String exportDirectory, String exportFormat) {
		def fosnrs = []
		if (fosnr == 'all') {
			def sql = Sql.newInstance(dburl)
			
			try {
				sql.eachRow(fosnrQuery) {row ->
					fosnrs << row.bfsnr
				}
			} catch (SQLException e) {
				log.error e.getMessage()
				throw new SQLException(e)
			} finally {
				sql.connection.close()
				sql.close()
			}
		} else {
			fosnrs << fosnr
		}
				
		// Datenumbau und Export...
		def config = ili2dbConfig()
		fosnrs.each() {bfsnr ->
			log.debug bfsnr
			
			
			
			
			
			
//			def fileName = exportDirectory + File.separator + bfsnr + "." + exportFormat
//			config.setXtffile(fileName)
//			Ili2db.runExport(config, "")
		}
	}
		
	private def dropSchema() {
		def sql = Sql.newInstance(dburl)
		sql.connection.autoCommit = false
							
		try {
			sql.execute("DROP SCHEMA IF EXISTS ${Sql.expand(this.dbschema)} CASCADE;")	
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
