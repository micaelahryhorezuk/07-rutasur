import { runBusinessService } from "../../business/runtime.js";
runBusinessService("notificaciones").catch(error => {
  console.error(JSON.stringify({ service: "notificaciones", message: error.stack ?? error.message }));
  process.exitCode = 1;
});
