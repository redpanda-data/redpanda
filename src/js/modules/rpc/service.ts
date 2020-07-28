import { Server } from "./server";

const port = 8124;
const service = new Server("", "", "");
service.listen(port);
