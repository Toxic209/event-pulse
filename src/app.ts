import Fastify, { type FastifyInstance } from "fastify"
import prisma from "./postgres/prisma.js";
import eventRoutes from "./api/modules/events/event.routes.js";

const app = Fastify({
    logger: true
});

app.decorate("prisma", prisma);

app.addHook("onClose", async (instance) => {
    await instance.prisma.$disconnect();
})

// --- Route Registers ---
async function global(fastify: FastifyInstance){
    fastify.register(eventRoutes, {
        prefix: "/events"
    });
}

app.register(global, {
    prefix: "/api/v1"
})

export default app;