import process from "process"

export const getConfig = () => {
	const env = process.env as Record<string, string>

	if (!env) {
		throw new Error("ENV could not be imported")
	}

	return {
		cosmos: {
			from: {
				connectionString: env["fromCosmosConnectionString"],
				databaseName: env["fromCosmosDatabaseName"],
				containerName: env["fromCosmosContainerName"]
			},
			to: {
				connectionString: env["toCosmosConnectionString"],
				databaseName: env["toCosmosDatabaseName"],
				containerName: env["toCosmosContainerName"]
			}
		}
	}
}

export type Config = ReturnType<typeof getConfig>
export type CosmosConfig = Config["cosmos"]
