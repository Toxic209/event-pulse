import app from "./app.js";
import dotenv from "dotenv"
dotenv.config({
    path: "./.env"
})

const port: number = Number(process.env.PORT ?? 4000);

app.listen({port}, (err) => {
    if(err){
        app.log.error(err);
        process.exit(1);
    }
    console.log(`Serving at ${port}`);
});