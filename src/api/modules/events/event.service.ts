import prisma from "../../../postgres/prisma.js";
import type eventData from "../../types/types.js";
import { ApiError } from "../../utils/ApiError.js";

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

        console.log(error);

        throw new ApiError({
            message: "Can't create Event in the database!",
            statusCode: 500,
            errorCode: "SERVER ERROR",
            details: error instanceof Error ? error.message : "Unknown Error"
        });

    }


}

export { createEvent }