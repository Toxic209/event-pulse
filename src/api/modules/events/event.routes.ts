import type { FastifyInstance } from "fastify";
import { createEventController } from "./event.controller.js";

const eventRoutes = async (fastify: FastifyInstance) => {
    fastify.post("/create-event", createEventController)
}

export default eventRoutes;