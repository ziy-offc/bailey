//=======================================================//
console.log(chalk.whiteBright("Hi, thank you for using my modified Baileys ^-^"));
console.log(chalk.cyan("Telegram: ") + chalk.greenBright("@ziyoffc1"));
console.log(chalk.cyan("ForUpdateBayleys: ") + chalk.greenBright("@ziyoffcch"));
import makeWASocket from "./Socket/index.js";
//=======================================================//
export * from "./Defaults/index.js";
export * from "./WABinary/index.js";
export * from "../WAProto/index.js";
export * from "./WAUSync/index.js";
export * from "./Store/index.js";
export * from "./Utils/index.js";
export * from "./Types/index.js";
export * from "./WAM/index.js";
//=======================================================//
export { makeWASocket };
export default makeWASocket;
//=======================================================//
