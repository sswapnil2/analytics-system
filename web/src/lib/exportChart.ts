/**
 * Chart export: the on-screen SVG, made standalone (computed colours inlined, since CSS variables
 * and classes don't exist outside the page), with a title and a legend, as SVG or a 2x PNG.
 */

const PAINT = [
  "fill",
  "stroke",
  "stroke-width",
  "stroke-dasharray",
  "stroke-linecap",
  "opacity",
  "fill-opacity",
  "stroke-opacity",
  "font-family",
  "font-size",
  "font-weight",
  "text-anchor",
  "dominant-baseline",
  "visibility",
];

function inlinePaint(src: Element, dst: Element) {
  const cs = getComputedStyle(src);
  const style = (dst as SVGElement).style;
  for (const p of PAINT) {
    const v = cs.getPropertyValue(p);
    if (v) style.setProperty(p, v);
  }
  style.removeProperty("transition");
  // Attributes like fill="rgb(var(--chart-1))" mean nothing outside the page; the inline style above replaces them.
  for (const attr of ["fill", "stroke", "class"]) if (dst.getAttribute(attr)?.includes("var(") || attr === "class") dst.removeAttribute(attr);
  for (let i = 0; i < src.children.length; i++) inlinePaint(src.children[i], dst.children[i]);
}

/** Turns a CSS colour like `rgb(var(--chart-1))` into a concrete one. */
function resolveColor(css: string): string {
  const el = document.createElement("span");
  el.style.color = css;
  document.body.appendChild(el);
  const out = getComputedStyle(el).color;
  el.remove();
  return out;
}

const esc = (s: string) => s.replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c]!);

export interface ChartExport {
  title: string;
  subtitle?: string;
  legend: { name: string; color: string }[];
}

export function chartSvgMarkup(svg: SVGSVGElement, { title, subtitle, legend }: ChartExport) {
  const box = svg.getBoundingClientRect();
  const w = Math.max(320, Math.round(box.width));
  const plotH = Math.round(box.height);
  const pad = 16;
  const font = getComputedStyle(document.body).fontFamily;
  const ink = resolveColor("rgb(var(--ink))");
  const muted = resolveColor("rgb(var(--muted))");
  const bg = resolveColor("rgb(var(--panel))");

  const clone = svg.cloneNode(true) as SVGSVGElement;
  inlinePaint(svg, clone);
  clone.setAttribute("x", String(pad));
  clone.setAttribute("y", String(subtitle ? 60 : 44));
  clone.setAttribute("width", String(w - pad * 2));
  clone.setAttribute("height", String(plotH));
  clone.removeAttribute("class");

  // Legend rows under the plot, wrapping at the image width.
  let x = pad;
  let y = (subtitle ? 60 : 44) + plotH + 24;
  const items: string[] = [];
  for (const l of legend) {
    const itemW = 18 + l.name.length * 7 + 16;
    if (x + itemW > w - pad && x > pad) {
      x = pad;
      y += 20;
    }
    items.push(
      `<rect x="${x}" y="${y - 9}" width="10" height="10" rx="2" fill="${resolveColor(l.color)}"/>` +
        `<text x="${x + 16}" y="${y}" font-size="12" fill="${ink}">${esc(l.name)}</text>`,
    );
    x += itemW;
  }
  const h = y + (legend.length ? 16 : -8);

  const markup =
    `<svg xmlns="http://www.w3.org/2000/svg" width="${w}" height="${h}" viewBox="0 0 ${w} ${h}" font-family="${esc(font)}">` +
    `<rect width="100%" height="100%" fill="${bg}"/>` +
    `<text x="${pad}" y="28" font-size="16" font-weight="600" fill="${ink}">${esc(title)}</text>` +
    (subtitle ? `<text x="${pad}" y="46" font-size="12" fill="${muted}">${esc(subtitle)}</text>` : "") +
    new XMLSerializer().serializeToString(clone) +
    items.join("") +
    `</svg>`;
  return { markup, width: w, height: h };
}

function save(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

export function downloadChartSvg(svg: SVGSVGElement, meta: ChartExport, filename: string) {
  save(new Blob([chartSvgMarkup(svg, meta).markup], { type: "image/svg+xml" }), `${filename}.svg`);
}

export async function downloadChartPng(svg: SVGSVGElement, meta: ChartExport, filename: string) {
  const { markup, width, height } = chartSvgMarkup(svg, meta);
  const url = URL.createObjectURL(new Blob([markup], { type: "image/svg+xml" }));
  try {
    const img = new Image();
    await new Promise<void>((ok, fail) => {
      img.onload = () => ok();
      img.onerror = () => fail(new Error("Couldn't draw the chart."));
      img.src = url;
    });
    const canvas = document.createElement("canvas");
    canvas.width = width * 2;
    canvas.height = height * 2;
    const ctx = canvas.getContext("2d");
    if (!ctx) throw new Error("Couldn't draw the chart.");
    ctx.scale(2, 2);
    ctx.drawImage(img, 0, 0);
    const blob = await new Promise<Blob | null>((ok) => canvas.toBlob(ok, "image/png"));
    if (!blob) throw new Error("Couldn't draw the chart.");
    save(blob, `${filename}.png`);
  } finally {
    URL.revokeObjectURL(url);
  }
}
