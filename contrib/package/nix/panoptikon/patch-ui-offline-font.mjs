// next/font/google → next/font/local + app/fonts/InterVariable.ttf (nix package).
import fs from "node:fs";

const path = process.argv[2];
if (!path) {
  console.error("usage: node patch-ui-offline-font.mjs <layout.tsx>");
  process.exit(2);
}

let t = fs.readFileSync(path, "utf8");

const fromImport = 'import { Inter as FontSans } from "next/font/google";';
const toImport = 'import localFont from "next/font/local";';
if (!t.includes(fromImport)) {
  throw new Error(`${path}: expected next/font/google Inter import`);
}
t = t.replace(fromImport, toImport);

const fromInit = [
  "const fontSans = FontSans({",
  '  subsets: ["latin"],',
  '  variable: "--font-inter",',
  "})",
].join("\n");

const toInit = [
  "const fontSans = localFont({",
  '  src: "./fonts/InterVariable.ttf",',
  '  variable: "--font-inter",',
  '  weight: "100 900",',
  '  display: "swap",',
  "})",
].join("\n");

if (!t.includes(fromInit)) {
  throw new Error(`${path}: expected FontSans({ subsets, variable }) init`);
}
t = t.replace(fromInit, toInit);
fs.writeFileSync(path, t);
