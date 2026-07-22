import Fastify from "fastify"
import prisma from "./postgres/prisma.js";

const app = Fastify({
    logger: true
});

app.decorate("prisma", prisma);

app.addHook("onClose", async (instance) => {
    await instance.prisma.$disconnect();
})

export default app;