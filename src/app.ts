import Fastify from "fastify"
import prisma from "./postgres/prisma.js";

const app = Fastify({
    logger: true
});

app.addHook("onClose", async () => {
    await prisma.$disconnect();
})

export default app;