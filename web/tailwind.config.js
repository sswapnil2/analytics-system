/** @type {import('tailwindcss').Config} */
const token = (name) => `rgb(var(--${name}) / <alpha-value>)`;

export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        canvas: token("canvas"),
        panel: token("panel"),
        sunken: token("sunken"),
        field: token("field"),
        hover: token("hover"),
        line: token("line"),
        "line-strong": token("line-strong"),
        ink: token("ink"),
        "ink-2": token("ink-2"),
        muted: token("muted"),
        faint: token("faint"),
        brand: token("brand"),
        "brand-hover": token("brand-hover"),
        "brand-soft": token("brand-soft"),
        "on-brand": token("on-brand"),
        danger: token("danger"),
        "danger-soft": token("danger-soft"),
        destructive: token("destructive"),
        "destructive-soft": token("destructive-soft"),
        warn: token("warn"),
        "warn-soft": token("warn-soft"),
        ok: token("ok"),
        "ok-soft": token("ok-soft"),
        info: token("info"),
        "info-soft": token("info-soft"),
      },
      fontFamily: {
        sans: ['"Instrument Sans Variable"', "system-ui", "-apple-system", "sans-serif"],
        mono: ["ui-monospace", "SFMono-Regular", "Menlo", "monospace"],
      },
    },
  },
  plugins: [],
};
