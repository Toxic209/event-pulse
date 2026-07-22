import { createClient } from "redis"
import dotenv from "dotenv"
dotenv.config({
    path: "./.env"
})

const redis = createClient({
    url: process.env.REDIS_URL as string
});

await redis.connect();

redis.on("error", (err) => console.error("Redis Error: ", err));

export default redis;