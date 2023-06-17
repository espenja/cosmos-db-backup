import path from "path"

import { Container, CosmosClient, QueryIterator } from "@azure/cosmos"
import dotenv from "dotenv"

import { Config, CosmosConfig, getConfig } from "./getConfig"

dotenv.config({
	path: path.resolve(__dirname, "./config.env")
})

const { log, error } = console

const fetchNextPage = async (query: QueryIterator<any>) => {
	if (!query) {
		throw new Error("Query has not been made yet. Exiting.")
	}

	const queryResponse = await query.fetchNext()
	return queryResponse
}

const performBackup = async (fromContainer: Container, toContainer: Container) => {
	const query = fromContainer.items.query("SELECT * from c")

	let done = false
	let response = await fetchNextPage(query)
	const promises: Array<Promise<void>> = []

	do {
		for (const document of response.resources) {
			// Create a wrapper promise. This lets us await multiple jobs at once.
			const promise = async () => {
				await this.backupSingleDocument(document, documentCleaner, documentTester, dryRun)
			}
			// Queue up the promise
			promises.push(promise())
		}

		// Await all wrapper promises
		await Promise.all(promises)
		this.log(`Done with page ${this.state.currentPage}`)

		if (response.hasMoreResults) {
			response = await this.fetchNextPage()
		} else {
			done = true
		}
	} while (!done)
}

const setupClients = (config: CosmosConfig) => {
	const fromClient = new CosmosClient({
		endpoint: config.from.fromCosmosEndpoint,
		key: config.from.fromCosmosKey
	})

	const toClient = new CosmosClient({
		endpoint: config.to.toCosmosEndpoint,
		key: config.to.toCosmosKey
	})

	const fromContainer = fromClient.database(config.from.fromCosmosDatabaseName).container(config.from.fromCosmosContainerName)
	const toContainer = toClient.database(config.to.toCosmosDatabaseName).container(config.to.toCosmosContainerName)

	return { fromContainer, toContainer }
}

const start = async () => {
	const { cosmos: config } = getConfig()
	const { fromContainer, toContainer } = setupClients(config)

	await performBackup(fromContainer, toContainer)
}

start().catch((err) => {
	error(err)
})
