package org.catais.geoig.ai.mopublic

import ch.ehi.ili2db.base.Ili2dbException
import groovy.util.logging.Log4j2
import groovy.util.CliBuilder

@Log4j2
class Main {

	//TODO: proper exception handling!!!!
	
	static main(args) {
		def cli = new CliBuilder(
			usage: 'java -jar XYZ.jar --initdb',
			header: '\nAvailable options (use --help for help):\n')
		cli.with {
			_ longOpt: 'help', 'Usage Information'
			_ longOpt: 'schemaimport', 'Prepare database by creating schema with empty tables', required: false
			_ longOpt: 'dropschema', 'Drop schema with all tables', required: false
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

		// Start logging here.
		def startTime = Calendar.instance.time
		def endTime
		log.info "Start: ${startTime}."
		
		try {
			if (options.schemaimport) {
				log.info 'Init database schema:'
				
	//			def pg = new PostgresqlDatabase([dbhost: 'localhost', dbport: '5432', dbdatabase: 'fubar'])
				def pg = new PostgresqlDatabase()
				pg.initSchema("av_mopublic_export")
				
				endTime = Calendar.instance.time
				log.debug "Elapsed time: ${(endTime.time - startTime.time)} ms"
				log.info 'Init database schema done.'
			}
		} catch (Ili2dbException e) {
			e.printStackTrace()
			log.error e.getMessage()
			log.error "Process aborted."
		} 
		
		
		endTime = Calendar.instance.time
		log.debug "Total elapsed time: ${(endTime.time - startTime.time)} ms"
		log.info "End: ${endTime}."
	}
}
