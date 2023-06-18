import path from "path"

import {
	Container,
	CosmosClient,
	FeedResponse,
	ItemDefinition,
	ItemResponse,
	QueryIterator,
	Resource,
	SqlQuerySpec
} from "@azure/cosmos"
import { ensureDirSync } from "fs-extra"

import { CosmosConfig } from "./config/getConfig"
import { createLogger, defaultFileLoggingProvider, SimpleLogger } from "./logging/simpleLogger"

export type CosmosUserResource = Resource & Record<string, any>

/**
 * Options for the function performBackup.
 */
export type BackupOptions = {
	/**
	 * The query that fetches the data to be backed up.
	 * @type {SqlQuerySpec}
	 */
	query: SqlQuerySpec

	/**
	 * The number of documents to fetch for each page of the query.
	 * @type {number}
	 */
	pageSize?: number

	/**
	 * If set to true, will do a dry run. This does not affect any of the databases.
	 * This value internally defaults to true.
	 * @type {boolean}
	 */
	dryRun?: boolean

	/**
	 * If set to single, will handle one document at a time.
	 * If set to "multi", will handle an entire page at a time.
	 * "multi" will be faster, but will use a lot more RUs than "single".
	 * "single" will be slower, but will use a lot less RUs than "multi".
	 * Defaults to "single".
	 * @type {("single" | "multi")}
	 */
	runMode?: "single" | "multi"
}

type PerformBackupOptions = Required<BackupOptions>

type BackupState = {
	currentPage: number
	currentDocument: number
	totalUsedRU: number
	numberOfBackups: number
	numberOfUpdates: number
	numberOfUnsuccessfulTests: number
	numberOfSuccessfulTests: number
	numberOfDocumentsLookedAt: number
}

/**
 * DatabaseBackup will perform a backup of documents matching a query provided.
 * @class DatabaseBackup
 */
export class CosmosDBBackup {
	private name: string
	private cosmosConfig: CosmosConfig

	private log: SimpleLogger
	private error: SimpleLogger

	private fromContainer: Container
	private toContainer: Container

	private partitionKey: string
	private query: QueryIterator<CosmosUserResource> | undefined = undefined

	private state: BackupState
	private defaultState: BackupState

	constructor(name: string, config: CosmosConfig, partitionKey: string) {
		this.name = name
		this.cosmosConfig = config
		this.partitionKey = partitionKey

		const logFilePath = path.resolve(__dirname, "log", `cosmos_backup_${this.name}_${new Date().getTime()}.log`)
		ensureDirSync(path.parse(logFilePath).dir)

		const { log, error } = createLogger("CosmosDBBackup", {
			useTimestamps: true,
			useColors: true,
			additionalLogProviders: [defaultFileLoggingProvider(logFilePath)]
		})

		this.log = log
		this.error = error

		const fromClient = new CosmosClient(config.from.connectionString)
		const toClient = new CosmosClient(config.to.connectionString)

		const fromContainer = fromClient.database(config.from.databaseName).container(config.from.containerName)
		const toContainer = toClient.database(config.to.databaseName).container(config.to.containerName)

		this.fromContainer = fromContainer
		this.toContainer = toContainer

		this.defaultState = this.state = {
			currentPage: 0,
			currentDocument: 0,
			totalUsedRU: 0,
			numberOfBackups: 0,
			numberOfSuccessfulTests: 0,
			numberOfUnsuccessfulTests: 0,
			numberOfUpdates: 0,
			numberOfDocumentsLookedAt: 0
		}

		log("CosmosDBBackup Class created")
	}

	/**
	 * Resets the internal state of DatabaseBackup.
	 * @private
	 * @memberof DatabaseBackup
	 */
	private resetState() {
		this.log("Resetting state")
		this.state = { ...this.defaultState }
	}

	/**
	 * Collects statistics from an ItemResponse or FeedResponse.
	 * @private
	 * @param {(ItemResponse<CosmosUserResource> | FeedResponse<CosmosUserResource> | undefined)} response
	 * @memberof DatabaseBackup
	 */
	private async collectResponseStats(
		response: ItemResponse<CosmosUserResource | ItemDefinition> | FeedResponse<CosmosUserResource> | undefined
	) {
		if (response) {
			this.state.totalUsedRU += parseFloat(response.requestCharge.toString())
		}
	}

	/**
	 * Performs a query against the main container and fetches the next page.
	 * @private
	 * @memberof DatabaseBackup
	 */
	private async fetchNextPage() {
		this.state.currentPage++
		this.log(`Fetching page ${this.state.currentPage}...`)

		if (!this.query) {
			throw new Error("Query has not been made yet. Exiting.")
		}

		const queryResponse = await this.query.fetchNext()
		this.collectResponseStats(queryResponse)

		return queryResponse
	}

	/**
	 * Backs up a document to the backup database and container.
	 * @private
	 * @param {CosmosUserResource} document
	 * @param {boolean} [dryRun]
	 * @memberof DatabaseBackup
	 */
	private async backupDocument(document: CosmosUserResource, dryRun?: boolean) {
		const backupDocument: CosmosUserResource = {
			...document,
			idOriginal: document.id,
			backupDate: new Date().toISOString(),
			backupJobName: this.name
		}

		this.log(
			`Backup document created with id '${backupDocument.id}' and partitionKey '${
				backupDocument[this.partitionKey]
			}'. Uploading...`
		)

		if (dryRun) {
			return undefined
		}

		const upsertResponse = await this.toContainer.items.upsert(backupDocument)
		this.log(
			`Backup document with id '${backupDocument.id}' and partitionKey '${
				backupDocument[this.partitionKey]
			}' uploaded to backup database`
		)
		this.collectResponseStats(upsertResponse)
		this.state.numberOfBackups++

		return backupDocument
	}

	/**
	 * Creates a Promise that performs a backup of the given document
	 * @private
	 * @param {CosmosUserResource} document
	 * @param {boolean} dryRun
	 * @memberof DatabaseBackup
	 */
	private async backupSingleDocument(document: CosmosUserResource, dryRun: boolean) {
		this.log(
			`Handling document #-${this.state.currentDocument} in page #-${this.state.currentPage} with id '${
				document.id
			}' and partitionKey '${document[this.partitionKey]}'`
		)

		await this.backupDocument(document, dryRun)
	}

	/**
	 * Performs backup in "single" mode
	 * @private
	 * @param {PerformBackupOptions} options
	 * @memberof DatabaseBackup
	 */
	private async performSingleModeBackup(options: PerformBackupOptions) {
		this.log(`Performing single mode backup`)
		this.resetState()

		const { dryRun } = options

		let done = false
		let response = await this.fetchNextPage()

		do {
			for (const document of response.resources) {
				this.state.currentDocument++
				this.state.numberOfDocumentsLookedAt++
				await this.backupSingleDocument(document, dryRun)
			}

			if (response.hasMoreResults) {
				response = await this.fetchNextPage()
			} else {
				done = true
			}
		} while (!done)
	}

	/**
	 * Performs backup in "multi" mode
	 * @private
	 * @param {PerformBackupOptions} options
	 * @memberof DatabaseBackup
	 */
	private async performMultiModeBackup(options: PerformBackupOptions) {
		const { dryRun } = options

		this.resetState()
		let done = false
		let response = await this.fetchNextPage()
		const promises: Array<Promise<void>> = []

		do {
			for (const document of response.resources) {
				// Create a wrapper promise. This lets us await multiple jobs at once.
				const promise = async () => {
					this.state.numberOfDocumentsLookedAt++
					this.state.currentDocument++
					await this.backupSingleDocument(document, dryRun)
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

	/**
	 * Performs backup
	 * @param {BackupOptions} options
	 * @memberof DatabaseBackup
	 */
	public async performBackup(options: BackupOptions) {
		const useOptions: PerformBackupOptions = {
			dryRun: true,
			runMode: "single",
			pageSize: 100,
			...options
		}

		this.query = this.fromContainer.items.query<CosmosUserResource>(useOptions.query, {
			maxItemCount: useOptions.pageSize
		})

		this.log("")
		this.log("==================================")
		this.log("")
		this.log(`Backup job '${this.name}' is starting. These settings will be used:`)
		this.log(`FROM database:\t${this.fromContainer.database.url} -> ${this.cosmosConfig.from.containerName}`)
		this.log(`TO database:\t${this.toContainer.database.url} -> ${this.cosmosConfig.to.containerName}`)
		this.log(`Query: ${useOptions.query.query.toString()}`)
		this.log(`${useOptions.dryRun ? "--> DRY RUN MODE TRUE <--" : "--> PRODUCTION MODE <--"}`)
		this.log("")
		this.log("==================================")
		this.log("")
		this.log("🙏")

		const startTime = new Date().getTime()

		try {
			if (useOptions.runMode === "single") {
				await this.performSingleModeBackup(useOptions)
			} else if (useOptions.runMode === "multi") {
				await this.performMultiModeBackup(useOptions)
			} else {
				throw new Error("Invalid runMode")
			}
		} catch (err) {
			this.error(err)
			throw err
		}

		const endTime = new Date().getTime()
		const timeUsedInMilliseconds = endTime - startTime
		const timeUsedInSeconds = timeUsedInMilliseconds / 1000

		this.log(`Total time used: ${timeUsedInMilliseconds}ms -> ${timeUsedInSeconds}s`)
		this.log(`Total amount of RU used: ${this.state.totalUsedRU}`)
		this.log(`Estimated amount of average RU/s: ${this.state.totalUsedRU / timeUsedInSeconds}`)
		this.log(`Total amount of pages: ${this.state.currentPage}`)
		this.log(`Total amount of documents looked at: ${this.state.numberOfDocumentsLookedAt}`)
		this.log(`Total amount of unsuccessful tests: ${this.state.numberOfUnsuccessfulTests}`)
		this.log(`Total amount of successful tests: ${this.state.numberOfSuccessfulTests}`)
		this.log(`Total amount of backups taken: ${this.state.numberOfBackups}`)
		this.log(`Total amount of updates: ${this.state.numberOfUpdates}`)
	}
}
