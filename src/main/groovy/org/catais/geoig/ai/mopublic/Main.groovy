package org.catais.geoig.ai.mopublic

import groovy.util.logging.Log4j2
import groovy.util.CliBuilder

@Log4j2
class Main {

	//TODO: proper exception handling!!!!
	
	static main(args) {
	
		def startTime = Calendar.instance.time
		def endTime
		def elapsedTime
		log.info "Start: ${startTime}."

		def cli = new CliBuilder(
			usage: 'java -jar XYZ.jar --initdb',
			header: '\nAvailable options (use --help for help):\n')
		cli.with {
			_ longOpt: 'help', 'Usage Information'
			_ longOpt: 'schemaimport', 'Prepare database by creating schema with empty tables', required: false
			_ longOpt: 'dropschema', 'Drop schema with all tables', required: false
		}
				
		def options= cli.parse(args)
		if (!options) {
			return
		}

		if (options.help) {
			cli.usage()
			return
		}

		if (options.schemaimport) {
			log.info 'Init database schema:'
			
//			def pg = new PostgresqlDatabase()			
//			pg.initSchema()
			
			endTime = Calendar.instance.time
			log.debug "Elapsed time: ${(endTime.time - startTime.time)} ms"
			log.info 'Init database schema done.'
		}

		
		
				
		endTime = Calendar.instance.time
		log.debug "Total elapsed time: ${(endTime.time - startTime.time)} ms"
		log.info "End: ${endTime}."
	}

}
