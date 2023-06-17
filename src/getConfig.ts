import process from "process"

export const getConfig = () => {
	const env = process.env as Record<string, string>

	if (!env) {
		throw new Error("ENV could not be imported")
	}

	return {
		cosmos: {
			from: {
				fromCosmosEndpoint: env["fromCosmosEndpoint"],
				fromCosmosKey: env["fromCosmosKey"],
				fromCosmosAccountName: env["fromCosmosAccountName"],
				fromCosmosDatabaseName: env["fromCosmosDatabaseName"],
				fromCosmosContainerName: env["fromCosmosContainerName"]
			},
			to: {
				toCosmosEndpoint: env["toCosmosEndpoint"],
				toCosmosKey: env["toCosmosKey"],
				toCosmosAccountName: env["toCosmosAccountName="],
				toCosmosDatabaseName: env["toCosmosDatabaseName="],
				toCosmosContainerName: env["toCosmosContainerName="]
			}
		}
	}
}

export type Config = ReturnType<typeof getConfig>
export type CosmosConfig = Config["cosmos"]
