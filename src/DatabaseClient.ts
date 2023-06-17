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

import { CosmosConfig } from "./getConfig"

export type UserDocument = Resource & Record<string, any>

/**
 * Options for the function performDataCleaningOptions.
 */
export type PerformDataCleaningOptions = {
	/**
	 * The query that fetches the data to be cleaned.
	 * @type {SqlQuerySpec}
	 */
	query: SqlQuerySpec

	/**
	 * The number of documents to fetch for each page of the query.
	 * @type {number}
	 */
	pageSize?: number

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

type CleanerState = {
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
 * @export
 * @class DatabaseBackup
 * @template TUserModel
 */
export class DatabaseBackup {
	private name: string
	private cosmosConfig: CosmosConfig

	private log: typeof console.log
	private error: typeof console.error
	private warn: typeof console.warn

	private fromContainer: Container
	private toContainer: Container

	private query: QueryIterator<UserDocument> | undefined = undefined

	private state: CleanerState
	private defaultState: CleanerState

	constructor(name: string, config: CosmosConfig) {
		this.name = name
		this.cosmosConfig = config

		const logFilePath = path.resolve(__dirname, "log", `dataCleanerRun-${this.name}-${new Date().getTime()}.log`)
		ensureDirSync(path.parse(logFilePath).dir)

		this.log = console.log
		this.error = console.error
		this.warn = console.warn

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
	}

	/**
	 * Resets the internal state of DataCleaner.
	 * @private
	 * @memberof DataCleaner
	 */
	private resetState() {
		this.log("Resetting state")
		this.state = { ...this.defaultState }
	}

	/**
	 * Collects statistics from an ItemResponse or FeedResponse.
	 * @private
	 * @param {(ItemResponse<UserDocument> | FeedResponse<UserDocument> | undefined)} response
	 * @memberof DataCleaner
	 */
	private async collectResponseStats(
		response: ItemResponse<UserDocument | ItemDefinition> | FeedResponse<UserDocument> | undefined
	) {
		if (response) {
			this.state.totalUsedRU += parseFloat(response.requestCharge.toString())
		}
	}

	/**
	 * Performs a query against the main container and fetches the next page.
	 * @private
	 * @memberof DataCleaner
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
	 * @param {UserDocument<TUserModel>} document
	 * @param {boolean} [dryRun]
	 * @memberof DataCleaner
	 */
	private async backupDocument(document: UserDocument, dryRun?: boolean) {
		const backupDocument: UserDocument = {
			...document,
			idOriginal: document.id,
			backupDate: new Date().toISOString(),
			backupJobName: this.name
		}

		this.log(`Backup document created with id '${backupDocument.id}'. Uploading...`)

		if (dryRun) {
			return undefined
		}

		const upsertResponse = await this.toContainer.items.upsert(backupDocument)
		this.log(`Backup document with id '${backupDocument.id}' uploaded to backup database`)
		this.collectResponseStats(upsertResponse)

		return backupDocument as UserDocument
	}

	/**
	 * Checks if a document exists.
	 * This is used to check if a document has been backed up or not.
	 * @private
	 * @param {CosmosDocument<TUserModel>} document
	 * @param {boolean} [dryRun]
	 * @memberof DataCleaner
	 */
	private async checkIfDocumentExists(document: CosmosDocument<TUserModel>, dryRun?: boolean) {
		if (dryRun) {
			return true
		}

		const { id } = document
		// Cosmos DB does not have a good way of simply checking if a document exists or not
		const query = this.toContainer.items.query<number>(
			`SELECT VALUE COUNT(1) FROM c WHERE c.id = '${id}' and c.${this.partitionKey} = '${document[this.partitionKey]}'`
		)

		const queryResponse = await query.fetchNext()
		this.collectResponseStats(queryResponse)

		const count = queryResponse.resources[0]
		const backupExists = count === 1
		this.log(backupExists ? `Backup document exists` : "Backup document does not exist")
		return backupExists
	}

	/**
	 * Replaces an existing document with a cleaned document
	 * @private
	 * @template TUserDocument
	 * @param {TUserDocument} oldDocument
	 * @param {TUserDocument} newDocument
	 * @param {boolean} [dryRun]
	 * @memberof DataCleaner
	 */
	private async replaceDocumentWithCleanedDocument<TUserDocument extends UserDocument<TUserModel>>(
		oldDocument: TUserDocument,
		newDocument: TUserDocument,
		dryRun?: boolean
	) {
		if (dryRun) {
			return undefined
		}

		const cleanedDocument: CleanedDocument<TUserModel> = {
			...newDocument,
			backupJobName: this.name
		}

		this.log(`Replacing document with id '${oldDocument.id}'`)
		const replaceResponse = await this.fromContainer.item(oldDocument.id, oldDocument[this.partitionKey]).replace(cleanedDocument)

		this.collectResponseStats(replaceResponse)
		return replaceResponse
	}

	/**
	 * Creates a Promise that performs both a backup of the given document,
	 * and replaces the old document with the cleaned document
	 * @private
	 * @param {CosmosDocument<TUserModel>} document
	 * @param {DocumentCleaner<TUserModel>} documentCleaner
	 * @param {DocumentTester<TUserModel>} documentTester
	 * @param {boolean} dryRun
	 * @memberof DataCleaner
	 */
	private async backupSingleDocument(document: any, dryRun: boolean) {
		this.log(
			`Handling document #-${this.state.currentDocument} in page #-${this.state.currentPage} with id '${
				document.id
			}' and partitionKey '${document[this.partitionKey]}'`
		)

		// Create deep copies of the document we want to clean

		const backupDocument = await this.backupDocument(document, dryRun)
		const backupExists = await this.checkIfDocumentExists(backupDocument!, dryRun)

		if (backupExists) {
			this.state.numberOfBackups++
			await this.replaceDocumentWithCleanedDocument(oldDocument, cleanedDocument, dryRun)
			this.state.numberOfUpdates++
		}
	}

	/**
	 * Performs testing and cleaning in "single" mode
	 * @private
	 * @param {CleaningOptions<TUserModel>} options
	 * @memberof DataCleaner
	 */
	private async performSingleModeCleaning(options: CleaningOptions<TUserModel>) {
		this.log(`Performing single mode cleaning`)
		this.resetState()

		const { documentCleaner, documentTester, dryRun } = options

		let done = false
		let response = await this.fetchNextPage()

		do {
			for (const document of response.resources) {
				this.state.currentDocument++
				this.state.numberOfDocumentsLookedAt++
				await this.backupSingleDocument(document, documentCleaner, documentTester, dryRun)
			}

			if (response.hasMoreResults) {
				response = await this.fetchNextPage()
			} else {
				done = true
			}
		} while (!done)
	}

	/**
	 * Performs testing and cleaning in "multi" mode
	 * @private
	 * @param {CleaningOptions<TUserModel>} options
	 * @memberof DataCleaner
	 */
	private async performMultiModeCleaning(options: CleaningOptions<TUserModel>) {
		const { documentCleaner, documentTester, dryRun } = options

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

	/**
	 * Countdown for starting the cleaning job
	 * @private
	 * @param {number} milliseconds
	 * @memberof DataCleaner
	 */
	private async countdownToTheApocalypse(milliseconds: number) {
		const timers: Array<Promise<any>> = []
		const totalSeconds = milliseconds / 1000

		for (let i = 0; i < totalSeconds; i++) {
			const timer = Promise.resolve(
				setTimeout(() => {
					this.log(`Starting data cleaning in ${totalSeconds - i} seconds`)
				}, i * 1000)
			)
			timers.push(timer)
		}

		return await Promise.all(timers)
	}

	/**
	 * Performs data cleaning
	 * @param {PerformDataCleaningOptions<TUserModel>} options
	 * @memberof DataCleaner
	 */
	public async performDataCleaning(options: PerformDataCleaningOptions<TUserModel>) {
		const useOptions: CleaningOptions<TUserModel> = {
			dryRun: true,
			runMode: "single",
			pageSize: 100,
			...options
		}

		this.query = this.fromContainer.items.query<CosmosDocument<TUserModel>>(useOptions.query, {
			maxItemCount: useOptions.pageSize
		})

		this.log("")
		this.log("==================================")
		this.log("")
		this.log(`Data cleaning job '${this.name}' is starting. These settings will be used:`)
		this.log(`FROM database:\t${this.cosmosConfig.cosmosDb.endpoint} -> ${this.cosmosConfig.cosmosDb.containerId}`)
		this.log(`TO database:\t${this.cosmosConfig.cosmosDbBackup.endpoint} -> ${this.cosmosConfig.cosmosDbBackup.containerId}`)
		this.log(`Query: ${useOptions.query.query.toString()}`)
		this.log(`${useOptions.dryRun ? "--> DRY RUN MODE TRUE <--" : "--> PRODUCTION MODE <--"}`)
		this.log("")
		this.log("==================================")
		this.log("")

		const timeToDie = 10 * 1000
		await this.countdownToTheApocalypse(timeToDie)

		setTimeout(async () => {
			this.log("🙏")
			const startTime = new Date().getTime()

			try {
				if (useOptions.runMode === "single") {
					await this.performSingleModeCleaning(useOptions)
				} else if (useOptions.runMode === "multi") {
					await this.performMultiModeCleaning(useOptions)
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
		}, timeToDie + 2 * 1000)
	}

	// TODO: Finish functionality to reverse data cleaning. Commented out until finished.
	// 	public async importFromBackup(options: ReverseDataCleaningOptions) {
	// 		const useOptions: ReverseDataCleaningOptions = {
	// 			dryRun: true,
	// 			runMode: "single",
	// 			pageSize: 100,
	// 			...options
	// 		}

	// 		const query = `
	// SELECT * FROM c
	// WHERE c.cleaningJobName = "${useOptions.cleaningOperationName}"
	// 		`

	// 		this.query = this.container.items.query<CosmosDocument<BackupDocument<TUserModel>>>(query, {
	// 			maxItemCount: useOptions.pageSize
	// 		})

	// 		this.log("")
	// 		this.log("==================================")
	// 		this.log("")
	// 		this.log(`Cleaning reversal job '${this.name}' is starting. These settings will be used:`)
	// 		this.log(
	// 			`BACKUP DATABASE:\t${this.cosmosConfig.cosmosDbBackup.endpoint} -> ${this.cosmosConfig.cosmosDbBackup.containerId}`
	// 		)
	// 		this.log(
	// 			`MAIN DATABASe:\t${this.cosmosConfig.cosmosDb.endpoint} -> ${this.cosmosConfig.cosmosDb.containerId}`
	// 		)
	// 		this.log(`Query: ${query}`)
	// 		this.log(`${useOptions.dryRun ? "--> DRY RUN MODE TRUE <--" : "--> PRODUCTION MODE <--"}`)
	// 		this.log("")
	// 		this.log("==================================")
	// 		this.log("")

	// 		const timeToDie = 10 * 1000
	// 		await this.countdownToTheApocalypse(timeToDie)
	// 	}
}
