import prisma from "../../postgres/prisma.js";
import type  eventData  from "../types/types.js";

const createEvent = async (eventData: eventData) => {
    const event = await prisma.event.create({
        data: eventData,
        select: {
            id: true,
            status: true
        }
    });

    return event;
}

export {createEvent}