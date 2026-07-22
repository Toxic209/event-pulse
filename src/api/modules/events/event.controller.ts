import { createEvent } from "./event.service.js";
import type eventData from "../../types/types.js";
import { ApiError } from "../../utils/ApiError.js";
import type { FastifyRequest, FastifyReply } from "fastify";

const createEventController = async (req: FastifyRequest<{ Body: eventData }>, reply: FastifyReply) => {
    const { eventType, payload } = req.body;

    if (!eventType) {
        throw new ApiError({
            message: "No event type declared. Event type is required for job processing.",
            statusCode: 400,
            errorCode: "BAD REQUEST"
        });
    }

    const createdEvent = await createEvent({ eventType, payload });

    if (!createdEvent) {
        throw new ApiError({
            message: "Something went wrong when creating the event in the database",
            statusCode: 500,
            errorCode: "INTERNAL SERVER ERROR"
        });
    }

    return reply.status(201).send(createEvent);

}