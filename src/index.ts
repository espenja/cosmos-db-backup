import path from "path"

import dotenv from "dotenv"

import { CosmosDBBackup } from "./CosmosDBBackup"
import { getConfig } from "./config/getConfig"
import { createLogger } from "./logging/simpleLogger"

dotenv.config({
	path: path.resolve(__dirname, "../.env")
})

const { log, error } = createLogger("CosmosDbBackup-Starter")

const start = async () => {
	const { cosmos: config } = getConfig()
	log("Starting backup job")
	const backup = new CosmosDBBackup("ContainerBackup", config, "pk")
	await backup.performBackup({
		query: {
			query: "SELECT * FROM c"
		},
		dryRun: false,
		pageSize: 100,
		runMode: "multi"
	})
}

start().catch((err) => {
	error(err)
})
