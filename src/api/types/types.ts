import type { Prisma } from "../../generated/prisma/client.js";

export default interface eventData {
    eventType: string,
    payload: Prisma.InputJsonValue,
}