import { runBusinessService } from "../../business/runtime.js";
runBusinessService("reprogramacion").catch(error => {
  console.error(JSON.stringify({ service: "reprogramacion", message: error.stack ?? error.message }));
  process.exitCode = 1;
});
