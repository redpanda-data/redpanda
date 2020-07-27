import { Server } from "./server";

const port: number = 8124;
const service = new Server("", "", "");
service.listen(port);
