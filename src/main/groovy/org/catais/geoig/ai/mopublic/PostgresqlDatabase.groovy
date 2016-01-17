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
	String dburl = "jdbc:postgresql://${dbhost}:${dbport}/${dbdatabase}"

	// TODO:
	// schemaimport with ili2 and export with ili1 does not work
	// schemaimport with ili2 and export with ili2 and *.itf does work
	// but exports wrong date format?
	// Workaround: init with ili1 for itf. Not sure if we need to adjust
	// sql queries (which would be a no-go... I guess).
	String modelName = "MOpublic03_ili2_v13"
	
	String fosnrQuery = "SELECT bfsnr FROM av_avdpool_ng.gemeindegrenzen_gemeinde;"
		
	void initSchema() {
		initSchema(dbschema)
	}
	
	void initSchema(String dbschema) {
		// 0) Drop cascade schema if exists.
		dropSchema(dbschema)
		log.debug "Existing schema dropped: ${dbschema}."
				
		// 1) Create schema and tables with ili2pg.
		def config = ili2dbConfig(dbschema)
		Ili2db.runSchemaImport(config, "");
		
		// 2) Insert functions into schema.
		// These functions are used to for data rebuilding (Datenumbau)
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
	
	void runExport(String dbschema, String fosnr, String exportDirectory, String exportFormat) {		
		// check if export format is supported
		if (!['itf', 'xtf', 'gml'].contains(exportFormat)) {
			throw new Exception("Export format is not supported: ${exportFormat}")
		}
				
		// We save the fosnr of thqe communities we want to export
		// in a list. If we export only one community its fosnr
		// is stored in a list too. After that we are able
		// to treat both cases equally (= for loop).
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
				
		// rebuilding and export...
		def config = ili2dbConfig(dbschema)
		config.setConfigReadFromDb(true)
	
		fosnrs.each() {bfsnr ->
			log.debug bfsnr
			
			// delete table content
			// SQLException will be thrown. Abort export!
			deleteFromTables(dbschema)
			
			// insert data into tables
			// SQLException will be thrown. Abort export!
			insertIntoTables(dbschema, bfsnr)
			
			// export as interlis
			def fileName = exportDirectory + File.separator + bfsnr + "." + exportFormat
			config.setXtffile(fileName)
			if (exportFormat == 'itf') {
				config.setItfTranferfile(true)
			}
			Ili2db.runExport(config, "")
			// TODO: there is no proper exception throwing in Ili2db...
		}
	}
	
	private def insertIntoTables(dbschema, fosnr) {
		def sql = Sql.newInstance(dburl)
		sql.connection.autoCommit = false
		
		try {
			sql.execute("SELECT ${Sql.expand(dbschema)}.update_mopublic('${Sql.expand(dbschema)}', ${Sql.expand(fosnr)});")	
			sql.commit()
			log.trace "Data inserted into tables."
		} catch (SQLException e) {
			sql.rollback()
			log.error e.getMessage()
			throw new SQLException(e)
		} finally {
			sql.connection.close()
			sql.close()
		}
	}
	
	private def deleteFromTables(dbschema) {
		def sql = Sql.newInstance(dburl)
		sql.connection.autoCommit = false
		
		try {
			def query = "SELECT * FROM ${Sql.expand(dbschema)}.t_ili2db_classname;"
			sql.eachRow(query) {row ->
				def tableName = row.sqlname.toLowerCase()
								
				def existsQuery = "SELECT EXISTS (SELECT * FROM information_schema.tables WHERE table_schema = '${Sql.expand(dbschema)}' " +
								"AND table_name = '${Sql.expand(tableName)}');"
				if (sql.firstRow(existsQuery).exists) {
					def deleteQuery = "DELETE FROM ${Sql.expand(dbschema)}.${Sql.expand(tableName)};"
					sql.execute(deleteQuery)
				}
			}
			sql.commit()
			log.trace "Data deleted: ${Sql.expand(dbschema)}.${Sql.expand(tableName)}"
		} catch (SQLException e) {
			sql.rollback()
			log.error e.getMessage()
			throw new SQLException(e)
		} finally {
			sql.connection.close()
			sql.close()
		}
	}
		
	private def dropSchema(dbschema) {
		def sql = Sql.newInstance(dburl)
		sql.connection.autoCommit = false
							
		try {
			sql.execute("DROP SCHEMA IF EXISTS ${Sql.expand(dbschema)} CASCADE;")	
			sql.commit()
			log.trace "Schema dropped: ${Sql.expand(dbschema)}}"
		} catch (SQLException e) {
			sql.rollback()
			log.error e.getMessage()
			throw new SQLException(e)
		} finally {
			sql.connection.close()
			sql.close()
		}		
	}
		
	private def ili2dbConfig(dbschema) {
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
