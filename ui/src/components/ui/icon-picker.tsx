import { useCallback, useEffect, useRef, useState, useMemo } from "react";
import { RotateCcw } from "lucide-react";
import { NODE_ICON_CATALOG } from "@/lib/icons";
import { cn } from "@/lib/utils";

interface IconPickerProps {
  currentIcon: string | null;
  currentImageProp: string | null;
  label: string;
  properties: string[];
  onSelectIcon: (iconName: string) => void;
  onResetIcon: () => void;
  onSelectImageProp: (prop: string) => void;
  onResetImageProp: () => void;
}

const CATEGORIES = [
  "People",
  "Places",
  "Objects",
  "Tech",
  "Science",
  "Nature",
  "Finance",
  "Media",
  "Misc",
];

export function IconPicker({
  currentIcon,
  currentImageProp,
  label,
  properties,
  onSelectIcon,
  onResetIcon,
  onSelectImageProp,
  onResetImageProp,
}: IconPickerProps) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  const handleClose = useCallback(() => setOpen(false), []);

  useEffect(() => {
    if (!open) return;
    function handleClickOutside(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        handleClose();
      }
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, [open, handleClose]);

  const currentIconData = useMemo(
    () => NODE_ICON_CATALOG.find((i) => i.name === currentIcon),
    [currentIcon],
  );

  const groupedIcons = useMemo(() => {
    const groups: Record<string, typeof NODE_ICON_CATALOG> = {};
    for (const cat of CATEGORIES) {
      groups[cat] = NODE_ICON_CATALOG.filter((i) => i.category === cat);
    }
    return groups;
  }, []);

  return (
    <span className="relative" ref={ref}>
      <button
        type="button"
        className="flex h-5 w-5 items-center justify-center rounded border border-border bg-background transition-colors hover:bg-accent"
        onClick={(e) => {
          e.stopPropagation();
          setOpen((v) => !v);
        }}
        title={`Icon for ${label}: ${currentIcon ?? "circle"}`}
      >
        {currentIconData && currentIconData.path ? (
          <svg viewBox="0 0 24 24" className="h-3.5 w-3.5 text-foreground">
            <path d={currentIconData.path} fill="currentColor" />
          </svg>
        ) : (
          <span className="block h-2.5 w-2.5 rounded-full bg-foreground/60" />
        )}
      </button>

      {open && (
        <div
          className="absolute left-0 top-6 z-50 w-64 rounded-md border border-border bg-popover p-2 shadow-lg"
          onClick={(e) => e.stopPropagation()}
        >
          {CATEGORIES.map((cat) => {
            const icons = groupedIcons[cat];
            if (!icons || icons.length === 0) return null;
            return (
              <div key={cat} className="mb-1.5">
                <div className="mb-0.5 text-[10px] font-medium uppercase tracking-wider text-muted-foreground">
                  {cat}
                </div>
                <div className="flex flex-wrap gap-0.5">
                  {icons.map((icon) => (
                    <button
                      key={icon.name}
                      type="button"
                      className={cn(
                        "flex h-8 w-8 items-center justify-center rounded transition-colors hover:bg-accent",
                        currentIcon === icon.name
                          ? "border-2 border-primary bg-accent"
                          : "border border-transparent",
                      )}
                      onClick={() => onSelectIcon(icon.name)}
                      title={icon.name}
                    >
                      {icon.path ? (
                        <svg
                          viewBox="0 0 24 24"
                          className="h-5 w-5 text-foreground"
                        >
                          <path d={icon.path} fill="currentColor" />
                        </svg>
                      ) : (
                        <span className="block h-3.5 w-3.5 rounded-full bg-foreground/60" />
                      )}
                    </button>
                  ))}
                </div>
              </div>
            );
          })}

          <button
            type="button"
            onClick={() => {
              onResetIcon();
              setOpen(false);
            }}
            className="mt-1 flex w-full items-center justify-center gap-1 rounded px-2 py-1 text-[11px] text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
          >
            <RotateCcw className="h-3 w-3" />
            Reset to Circle
          </button>

          {properties.length > 0 && (
            <>
              <div className="my-1.5 border-t border-border" />
              <div className="text-[10px] font-medium uppercase tracking-wider text-muted-foreground">
                Image from Property
              </div>
              <select
                className="mt-1 w-full rounded border border-border bg-background px-1.5 py-1 text-[11px] text-foreground outline-none focus:ring-1 focus:ring-ring"
                value={currentImageProp ?? ""}
                onChange={(e) => {
                  const val = e.target.value;
                  if (val === "") {
                    onResetImageProp();
                  } else {
                    onSelectImageProp(val);
                  }
                }}
              >
                <option value="">None</option>
                {properties.map((prop) => (
                  <option key={prop} value={prop}>
                    {prop}
                  </option>
                ))}
              </select>
            </>
          )}
        </div>
      )}
    </span>
  );
}
