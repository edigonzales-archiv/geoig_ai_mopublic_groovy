package org.catais.geoig.ai.mopublic

import ch.ehi.ili2db.base.Ili2dbException
import groovy.util.logging.Log4j2
import groovy.util.CliBuilder
import java.sql.SQLException

@Log4j2
class Main {

	//TODO: proper exception handling!!!!

	static main(args) {
		// some defaults
		def dbschema = 'av_mopublic_export'
		def exportFormat = 'xtf'
		def exportDirectory = System.getProperty('java.io.tmpdir')
	
		def cli = new CliBuilder(
				usage: 'java -jar XYZ.jar --initdb',
				header: '\nAvailable options (use --help for help):\n')
		cli.with {
			_ longOpt: 'help', 'Usage Information'
			_ longOpt: 'schemaimport', 'Prepare database by creating schema with empty tables', required: false
			_ longOpt: 'export', 'Export MOpublic transfer file.', args:1, argName:'fosnr'
			_ longOpt: 'dbschema', "The name of the schema in the database. Defaults to ${dbschema}", required: false, args:1, argName:'schema'
			_ longOpt: 'format', "Export format: xtf, gml, itf. Defaults to ${exportFormat}.",  args:1, argName:'format'
			_ longOpt: 'directory', 'Directory where exports will be written to. Defaults to java.io.tmpdir.', args:1, argName:'dir'
		}

		def options= cli.parse(args)

		if (args.size() == 0) {
			cli.usage()
			return
		}

		if (!options) {
			return
		}

		if (options.help) {
			cli.usage()
			return
		}
		
		if (options.format) {
			exportFormat = options.format
		}
		
		if (options.directory) {
			exportDirectory = options.directory
		}

		if (options.dbschema) {
			dbschema = options.dbschema
		}
		
		// Start logging here.
		def startTime = Calendar.instance.time
		def endTime
		log.info "Start: ${startTime}."

		try {
			if (options.schemaimport) {
				log.info 'Init database schema:'

//				def pg = new PostgresqlDatabase([dbhost: 'localhost', dbport: '5432', dbdatabase: 'fubar'])
				def pg = new PostgresqlDatabase()
				
				// dbschema als parameter?
//				pg.initSchema(dbschema)

				endTime = Calendar.instance.time
				log.debug "Elapsed time: ${(endTime.time - startTime.time)} ms"
				log.info 'Init database schema done.'
			}

			if (options.export) {
				log.info 'Export to mopublic transfer file:'
				
				def pg = new PostgresqlDatabase()
				
				// options.export is either a fosnr (=int)
				// or a string ('all'). Everything else
				// is not valid.
				try {
					def fosnr = options.export as int
				} catch (NumberFormatException e) {
					if (options.export != 'all') {
						throw new Exception("No valid export parameter: ${options.export}")
					}
				}
				
				// TODO: export dir?!
				// dbparameter als option.				
				pg.runExport(dbschema, options.export, exportDirectory, exportFormat)
				
				endTime = Calendar.instance.time
				log.debug "Elapsed time: ${(endTime.time - startTime.time)} ms"
				log.info 'Export done.'
			}



		} catch (Ili2dbException e) {
			e.printStackTrace()
			log.error e.getMessage()
			log.error "Process aborted."
		} catch (SQLException e) {
			e.printStackTrace()
			log.error e.getMessage()
			log.error "Process aborted."
		} catch (Exception e) {
			e.printStackTrace()
			log.error e.getMessage()
			log.error "Process aborted."
		}


		endTime = Calendar.instance.time
		log.debug "Total elapsed time: ${(endTime.time - startTime.time)} ms"
		log.info "End: ${endTime}."
	}
}
