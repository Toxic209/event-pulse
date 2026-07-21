export class ApiError extends Error {
    public readonly statusCode: number
    public readonly errorCode: string
    public readonly details?: unknown

    constructor({message, statusCode, errorCode, details}: {
        message: string,
        statusCode: number,
        errorCode: string,
        details?: string
    }) {
        super(message);

        this.statusCode = statusCode,
        this.errorCode = errorCode,
        this.details = details

        Error.captureStackTrace(this, this.constructor);
    }
}