const major = parseInt(process.versions.node.split('.')[0], 10);

if (major < 20) {
      console.error(
    `\n❌ Versi nodejs tidak valid mohon ubah ke Node.js 20+\n` +
    `   Anda menggunakan Node.js ${process.versions.node}.\n` +
    `   Silakan upgrade ke Node.js 20+ untuk melanjutkan.\n`
  );
  process.exit(1);
}