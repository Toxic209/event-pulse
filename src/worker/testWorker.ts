import redis from "../redis/redis.js";

const worker = async () => {

    while (true) {
        console.log("worker active...");

        async function ensureGroupCreation(){
            try {
                await redis.xGroupCreate(
                    "event",
                    "workers",
                    "$",
                    {
                        MKSTREAM: true
                    }
                )
            } catch (error: any) {
                if(!error.message.includes("BUSYGROUP")){
                    throw error;
            }
        }}

        await ensureGroupCreation();

        const consumerName = process.argv[2] || "worker-1"

        const messages = await redis.xReadGroup("workers", consumerName, {
            key: "event",
            id: ">"
        },
            {
                BLOCK: 0
            });
    
            if(messages){
                for(let stream of messages){
                    for(let msg of stream.messages){
                        console.log(msg.id);
                        await new Promise(resolve => setTimeout(resolve, 5000));
                        const data = JSON.parse(msg.message.payload);
                        console.log(data);
                        redis.xAck("event", "workers", msg.id);
                    }
                }
            }
    }

}


worker();