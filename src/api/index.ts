import app from "./app.js";
import dotenv from "dotenv"
import prisma from "./postgres/prisma.js";
dotenv.config({
    path: "./.env"
})

const port: number = Number(process.env.PORT ?? 4000);

app.listen({port}, (err) => {
    try {
        if(err){
            app.log.error(err);
            process.exit(1);
        }
        console.log(`Serving at ${port}`);
    } catch (error) {
        console.log(error)
        throw error;
    }
});

app.addHook("onClose", async () => {
    await prisma.$disconnect();
})