// Screenshots of the console, light and dark, from a running server — for the docs and for review.
// Usage: RUSTCDC_URL=http://127.0.0.1:8080 RUSTCDC_READ_TOKEN=… OUT=../../../site/static/console \
//          PLACES=health,snapshots,activity,shapes/<id> NAME=prefix node scripts/shots.mjs
// Writes <name><place>-<scheme>.webp.
import puppeteer from "puppeteer-core";

const url = process.env.RUSTCDC_URL ?? "http://127.0.0.1:8080";
const out = process.env.OUT ?? "shots";
const chrome = process.env.CHROME ?? "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
const browser = await puppeteer.launch({ executablePath: chrome, headless: true, args: ["--force-device-scale-factor=2"] });
const page = await browser.newPage();
await page.setViewport({ width: 1280, height: 800, deviceScaleFactor: 2 });

for (const scheme of ["light", "dark"]) {
  await page.emulateMediaFeatures([{ name: "prefers-color-scheme", value: scheme }]);
  await page.goto(`${url}/ui/`, { waitUntil: "networkidle0" });
  if (process.env.RUSTCDC_READ_TOKEN && (await page.$("#token"))) {
    await page.type("#token", process.env.RUSTCDC_READ_TOKEN);
    await page.keyboard.press("Enter");
  }
  for (const place of (process.env.PLACES ?? "health,snapshots,activity,shapes").split(",")) {
    await page.evaluate((p) => (location.hash = `#/${p}`), place);
    await new Promise((r) => setTimeout(r, Number(process.env.WAIT ?? 4500)));
    const file = `${process.env.NAME ?? ""}${place.split("/")[0]}-${scheme}.webp`;
    await page.screenshot({ path: `${out}/${file}`, type: "webp", quality: 82 });
  }
}
await browser.close();
