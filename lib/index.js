"use strict";

const chalk = require("chalk");

const clearConsole = () => {
  process.stdout.write(
    process.platform === "win32" ? "\x1B[2J\x1B[0f" : "\x1B[2J\x1B[3J\x1B[H"
  );
};

clearConsole();

// ============================
// ENHANCED BANNER DESIGN
// ============================

// Fungsi untuk membuat gradient text
const gradientText = (text, colors) => {
  const chars = text.split('');
  return chars.map((char, i) => {
    const colorIndex = Math.floor((i / chars.length) * (colors.length - 1));
    return chalk.hex(colors[colorIndex])(char);
  }).join('');
};

// Animasi garis dengan efek
const createAnimatedLine = (length = 60, color1 = '#ff6ec7', color2 = '#7873f5') => {
  let line = '';
  for (let i = 0; i < length; i++) {
    const ratio = i / length;
    const r = Math.floor(parseInt(color1.slice(1, 3), 16) * (1 - ratio) + parseInt(color2.slice(1, 3), 16) * ratio);
    const g = Math.floor(parseInt(color1.slice(3, 5), 16) * (1 - ratio) + parseInt(color2.slice(3, 5), 16) * ratio);
    const b = Math.floor(parseInt(color1.slice(5, 7), 16) * (1 - ratio) + parseInt(color2.slice(5, 7), 16) * ratio);
    line += chalk.rgb(r, g, b)('━');
  }
  return line;
};

// Banner utama dengan desain modern


// Footer dengan icon dekoratif
console.log(chalk.hex('#7873f5')(`
  ✦⋅⋆────────────────────────────────────────⋆⋅✦
`));
console.log(chalk.hex("#6f00f")(" W E L C O M E\n"));
console.log(chalk.hex("#6f00f")("Telegram : @ziyoffc1\n"));
console.log(chalk.hex("#6f00f")("Ch Telegram: t.me/ziyoffcch\n"));
console.log(chalk.hex("#6f00f")("Ch Whaatap: https://whatsapp.com/channel/0029VbB1vFaAYlULfsTtXU2r\n"));
console.log(chalk.hex("#6f00f")("Ch TikTok: https://www.tiktok.com/@fahreziiy?_r=1&_t=ZS-925T3D3pvzc\n"));
console.log(chalk.hex("#6f00f")("Ch YouTube: https://youtube.com/@ziyoffc_id?si=-xhHyWRywKPc4r79\n"));
console.log(chalk.hex("#6f00f")("Bayliyes: github:ziy-offc/bailey\n"));
console.log(chalk.hex("#6f00f")("FOLLOW SOCIAL MEDIA UNTUK INFORMASI UPDATE BAYLIYES\n"));
console.log(chalk.hex('#7873f5')(`
 ✦⋅⋆────────────────────────────────────────⋆⋅✦
`));

// ============================
// FIXED __createBinding
// ============================

var createBinding =
  (this && this.createBinding) ||
  (Object.create
    ? function (o, m, k, k2) {
        if (k2 === undefined) k2 = k;
        var desc = Object.getOwnPropertyDescriptor(m, k);

        if (
          !desc ||
          (!("get" in desc) && (desc.writable || desc.configurable))
        ) {
          desc = {
            enumerable: true,
            get: function () {
              return m[k];
            },
          };
        }

        Object.defineProperty(o, k2, desc);
      }
    : function (o, m, k, k2) {
        if (k2 === undefined) k2 = k;
        o[k2] = m[k];
      });

var exportStar =
  (this && this.exportStar) ||
  function (m, exports) {
    for (var p in m)
      if (p !== "default" && !Object.prototype.hasOwnProperty.call(exports, p))
        createBinding(exports, m, p);
  };

var importDefault =
  (this && this.importDefault) ||
  function (mod) {
    return mod && mod.__esModule ? mod : { default: mod };
  };

Object.defineProperty(exports, "__esModule", { value: true });

const Socket_1 = importDefault(require("./Socket"));

exports.makeWASocket = Socket_1.default;

exportStar(require("../WAProto"), exports);
exportStar(require("./Utils"), exports);
exportStar(require("./Types"), exports);
exportStar(require("./Store"), exports);
exportStar(require("./Defaults"), exports);
exportStar(require("./WABinary"), exports);
exportStar(require("./WAM"), exports);
exportStar(require("./WAUSync"), exports);

exports.default = Socket_1.default;
