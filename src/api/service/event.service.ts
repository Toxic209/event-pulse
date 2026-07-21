import prisma from "../../postgres/prisma.js";
import type  eventData  from "../types/types.js";
import { ApiError } from "../utils/ApiError.js";

const createEvent = async (eventData: eventData) => {
    try {
        const event = await prisma.event.create({
            data: eventData,
            select: {
                id: true,
                status: true
            }
        });

        //this section will deal with redis event pushes later.
        
        return event;
    } catch (error) {
        throw error;
    }


}

export {createEvent}