import app from "./app.js";

const port = 3000;

app.listen({port}, (err) => {
    if(err){
        app.log.error(err);
        process.exit(1);
    }
    console.log(`Serving at ${port}`);
})