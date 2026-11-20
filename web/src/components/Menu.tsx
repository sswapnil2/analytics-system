import React, { useEffect, useRef, useState } from "react";

export interface MenuItem {
  label: string;
  onSelect?: () => void;
  href?: string;
  danger?: boolean;
  disabled?: boolean;
  /** Set on choice menus: the item is a radio and this marks the current choice. */
  checked?: boolean;
  icon?: React.ComponentType<{ className?: string }>;
  /** Short note shown beside the label, e.g. why it is disabled. */
  hint?: string;
  /** Items with different groups get a divider between them. */
  group?: string;
}

/** Small dropdown menu: closes on outside click or Escape. */
export const Menu: React.FC<{
  label: string;
  trigger: React.ReactNode;
  triggerClass: string;
  items: MenuItem[];
  /** Which edge of the trigger the menu lines up with. */
  align?: "left" | "right";
}> = ({ label, trigger, triggerClass, items, align = "right" }) => {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const menuRef = useRef<HTMLDivElement>(null);
  const close = (refocus: boolean) => {
    setOpen(false);
    if (refocus) triggerRef.current?.focus();
  };
  const menuItems = () =>
    Array.from(menuRef.current?.querySelectorAll<HTMLElement>('[role^="menuitem"]:not([disabled])') ?? []);
  // Arrow keys move through items, Home/End jump, Escape returns focus to the button.
  const onMenuKey = (e: React.KeyboardEvent) => {
    const list = menuItems();
    const i = list.indexOf(document.activeElement as HTMLElement);
    const go = (n: number) => {
      e.preventDefault();
      list[(n + list.length) % list.length]?.focus();
    };
    if (e.key === "ArrowDown") go(i + 1);
    else if (e.key === "ArrowUp") go(i - 1);
    else if (e.key === "Home") go(0);
    else if (e.key === "End") go(list.length - 1);
    else if (e.key === "Escape") {
      e.preventDefault();
      close(true);
    } else if (e.key === "Tab") close(false);
  };
  useEffect(() => {
    if (open) menuItems()[0]?.focus();
  }, [open]);
  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && setOpen(false);
    const onDown = (e: MouseEvent) => ref.current && !ref.current.contains(e.target as Node) && setOpen(false);
    window.addEventListener("keydown", onKey);
    window.addEventListener("mousedown", onDown);
    return () => {
      window.removeEventListener("keydown", onKey);
      window.removeEventListener("mousedown", onDown);
    };
  }, [open]);
  const itemClass = (danger?: boolean) =>
    `block w-full whitespace-nowrap rounded-md px-3 py-2 text-left text-sm outline-none disabled:cursor-not-allowed disabled:opacity-45 ${
      danger ? "text-destructive hover:bg-destructive-soft focus:bg-destructive-soft" : "text-ink hover:bg-hover focus:bg-hover"
    }`;
  return (
    <div className="relative" ref={ref}>
      <button
        ref={triggerRef}
        type="button"
        className={triggerClass}
        aria-label={label}
        aria-haspopup="menu"
        aria-expanded={open}
        onClick={() => setOpen((v) => !v)}
        onKeyDown={(e) => {
          if (e.key === "ArrowDown" && !open) {
            e.preventDefault();
            setOpen(true);
          }
        }}
      >
        {trigger}
      </button>
      {open && (
        <div ref={menuRef} role="menu" aria-label={label} onKeyDown={onMenuKey} className={`absolute ${align === "right" ? "right-0 origin-top-right" : "left-0 origin-top-left"} top-full z-40 mt-1 min-w-[12rem] rounded-xl border border-line-strong bg-panel p-1 shadow-pop motion-safe:animate-pop`}>
          {items.map((it, i) => (
            <React.Fragment key={it.label}>
            {it.group && i > 0 && items[i - 1].group !== it.group && <div role="separator" className="mx-2 my-1 h-px bg-line" />}
            {it.href ? (
              <a role="menuitem" tabIndex={-1} href={it.href} className={itemClass(it.danger)} onClick={() => setOpen(false)}>
                {it.label}
              </a>
            ) : (
              <button
                type="button"
                role={it.checked === undefined ? "menuitem" : "menuitemradio"}
                aria-checked={it.checked}
                tabIndex={-1}
                disabled={it.disabled}
                title={it.hint}
                className={itemClass(it.danger)}
                onClick={() => {
                  close(true);
                  it.onSelect?.();
                  // Removing a card unmounts this menu's button; keep keyboard focus on the page.
                  requestAnimationFrame(() => {
                    if (!triggerRef.current?.isConnected) document.getElementById("main")?.focus();
                  });
                }}
              >
                <span className="flex items-center gap-2.5">
                  {it.icon && <it.icon className="h-4 w-4 shrink-0 text-muted" />}
                  <span className="flex flex-1 flex-col">
                    <span className={it.checked ? "font-semibold text-brand" : ""}>{it.label}</span>
                    {it.hint && <span className="text-xs text-muted">{it.hint}</span>}
                  </span>
                </span>
              </button>
            )}
            </React.Fragment>
          ))}
        </div>
      )}
    </div>
  );
};

