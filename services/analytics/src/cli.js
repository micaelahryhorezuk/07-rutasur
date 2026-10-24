import { runBusinessService } from "../../business/runtime.js";
runBusinessService("analitica").catch(error => {
  console.error(JSON.stringify({ service: "analitica", message: error.stack ?? error.message }));
  process.exitCode = 1;
});
