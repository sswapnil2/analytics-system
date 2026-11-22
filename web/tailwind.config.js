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
      // Motion tokens: fast, decelerating, so state changes read without waiting on them.
      transitionTimingFunction: {
        out: "cubic-bezier(0.23, 1, 0.32, 1)",
        drawer: "cubic-bezier(0.32, 0.72, 0, 1)",
      },
      keyframes: {
        "fade-in": { from: { opacity: "0" }, to: { opacity: "1" } },
        pop: { from: { opacity: "0", transform: "translateY(-4px) scale(0.98)" }, to: { opacity: "1", transform: "none" } },
        "slide-in-right": { from: { transform: "translateX(24px)", opacity: "0" }, to: { transform: "none", opacity: "1" } },
      },
      animation: {
        "fade-in": "fade-in 200ms cubic-bezier(0.23, 1, 0.32, 1)",
        pop: "pop 140ms cubic-bezier(0.23, 1, 0.32, 1)",
        "slide-in-right": "slide-in-right 240ms cubic-bezier(0.32, 0.72, 0, 1)",
      },
      boxShadow: {
        pop: "0 12px 32px rgb(0 0 0 / 0.16)",
      },
      // Type scale additions (no line-height, so they inherit like the arbitrary values they replaced).
      // Full scale: 2xs 11 · xs 12 · compact 13 · sm 14 · md 15 · then titles. See docs/design/design-system.md.
      fontSize: {
        "2xs": "0.6875rem",
        compact: "0.8125rem",
        md: "0.9375rem",
      },
      fontFamily: {
        sans: ['"Instrument Sans Variable"', "system-ui", "-apple-system", "sans-serif"],
        mono: ["ui-monospace", "SFMono-Regular", "Menlo", "monospace"],
      },
    },
  },
  plugins: [],
};
