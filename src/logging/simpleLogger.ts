import { error as cerror, log as clog, warn as cwarn } from "console"
import path from "path"

import colors, { Color } from "colors"
import { appendFileSync, ensureFileSync, PathLike } from "fs-extra"

export type SimpleLogger = (message?: any, obj?: any, optionsOverride?: SimpleLoggerOptions) => void
export type SimpleLoggerOptions = {
	name?: string
	useLoggerName?: boolean
	useTimestamps?: boolean
	doLog?: boolean
	useColors?: boolean
	dateStringOverride?: () => string
	objFormatter?: (obj: any) => string
	additionalLogProviders?: Array<SimpleLogger>
}

const getTimeStampString = (options: SimpleLoggerOptions = {}) => {
	return options.useTimestamps
		? `${options.dateStringOverride ? options.dateStringOverride() : new Date().toLocaleTimeString()} -> `
		: ""
}

export const createLogger = (name: string, options?: SimpleLoggerOptions) => {
	const loggerOptions = {
		name,
		useLoggerName: true,
		useTimestamps: true,
		doLog: true,
		useColors: false,
		...options
	} as SimpleLoggerOptions

	const write = (
		logger: typeof clog | typeof cerror | typeof cwarn,
		color: Color,
		message?: string,
		obj?: any,
		optionsOverride?: SimpleLoggerOptions
	) => {
		if (!loggerOptions.doLog) return

		const useOptions = { ...loggerOptions, ...optionsOverride }

		const messageStr = message ? message : ""
		const nameStr = useOptions.useLoggerName ? `${name}` : ""
		const objStr = useOptions.objFormatter ? useOptions.objFormatter(obj) : obj
		const timeStampStr = getTimeStampString(options)

		let logMessage = messageStr

		if (useOptions.useColors) {
			logMessage = `${color.green(timeStampStr)}${color(nameStr)}: ${messageStr}`
		} else {
			logMessage = `${timeStampStr}${nameStr}: ${messageStr}`
		}

		logger(logMessage)

		if (objStr) {
			console.dir(objStr, { depth: null })
		}

		if (options?.additionalLogProviders?.length) {
			for (const additionalLogProvider of options?.additionalLogProviders || []) {
				additionalLogProvider(message, obj, useOptions)
			}
		}
	}

	const log: SimpleLogger = (message, obj?, optionsOverride?) => {
		write(console.log, colors.blue, message, obj, optionsOverride)
	}

	const error: SimpleLogger = (message, obj?, optionsOverride?) => {
		write(console.error, colors.red, message, obj, optionsOverride)
	}

	const warn: SimpleLogger = (message, obj?, optionsOverride?) => {
		write(console.warn, colors.yellow, message, obj, optionsOverride)
	}

	return { log, error, warn }
}

/**
 * @param {PathLike} logFileLocation Path to log file
 */
export type FileLogProvider = (logFileLocation: PathLike) => SimpleLogger

export const defaultFileLoggingProvider: FileLogProvider = (logFileLocation) => {
	const logFilePath = path.resolve(logFileLocation.toString())

	const ensureDirAndFileExists = () => {
		ensureFileSync(logFilePath)
	}

	return (message?: any, obj?: any, options?: SimpleLoggerOptions) => {
		const messageStr = message
		const timeStampStr = getTimeStampString({
			...options,
			useTimestamps: true
		})
		const nameStr = options?.useLoggerName ? `${options.name}` : ""
		const logMessage = `${timeStampStr}${nameStr}: ${messageStr}`

		// Log message
		if (message) {
			ensureDirAndFileExists()
			appendFileSync(logFilePath, logMessage + "\n", {
				encoding: "utf8"
			})
		}
		// Log object
		if (obj) {
			ensureDirAndFileExists()
			appendFileSync(logFilePath, JSON.stringify(obj, undefined, 2) + "\n", {
				encoding: "utf8"
			})
		}
	}
}
