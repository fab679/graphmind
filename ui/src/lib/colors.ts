import { useGraphSettingsStore } from "../stores/graphSettingsStore";

const LABEL_COLORS: Record<string, string> = {
  Person: "#6366f1",
  User: "#6366f1",
  Employee: "#6366f1",
  Supplier: "#3b82f6",
  Company: "#3b82f6",
  Organization: "#3b82f6",
  Port: "#f43f5e",
  Factory: "#f43f5e",
  Location: "#f43f5e",
  Shipment: "#10b981",
  Material: "#10b981",
  Product: "#10b981",
  Disease: "#ef4444",
  Condition: "#ef4444",
  Drug: "#06b6d4",
  Medication: "#06b6d4",
  Compound: "#8b5cf6",
  Gene: "#8b5cf6",
  Movie: "#f59e0b",
  Actor: "#ec4899",
  Director: "#14b8a6",
  Account: "#f97316",
  Transaction: "#84cc16",
  Server: "#64748b",
  Alert: "#f43f5e",
};

const FALLBACK_PALETTE = [
  "#6366f1",
  "#3b82f6",
  "#10b981",
  "#f59e0b",
  "#ef4444",
  "#8b5cf6",
  "#ec4899",
  "#14b8a6",
  "#f97316",
  "#06b6d4",
  "#84cc16",
  "#64748b",
];

function hashString(str: string): number {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    hash = (hash << 5) - hash + str.charCodeAt(i);
    hash |= 0;
  }
  return Math.abs(hash);
}

export function getColorForLabel(label: string): string {
  if (label in LABEL_COLORS) {
    return LABEL_COLORS[label];
  }
  const index = hashString(label) % FALLBACK_PALETTE.length;
  return FALLBACK_PALETTE[index];
}

const DISPLAY_NAME_KEYS = [
  "name",
  "title",
  "hostname",
  "username",
  "email",
  "label",
  "displayName",
  "display_name",
  "identifier",
  "id",
];

export function getNodeDisplayName(
  properties: Record<string, unknown>,
): string {
  for (const key of DISPLAY_NAME_KEYS) {
    const value = properties[key];
    if (typeof value === "string" && value.length > 0) {
      return value;
    }
  }

  const keys = Object.keys(properties);
  for (const key of keys) {
    const value = properties[key];
    if (typeof value === "string" && value.length > 0) {
      return value;
    }
  }

  return "?";
}

const DEFAULT_EDGE_COLOR = "#64748b";

/**
 * Returns the custom color for a node label if one is set in the graph settings store,
 * otherwise falls back to the default color from getColorForLabel.
 */
export function getCustomColorForLabel(label: string): string {
  const custom = useGraphSettingsStore.getState().labelColors[label];
  if (custom) return custom;
  return getColorForLabel(label);
}

/**
 * Returns the custom color for an edge type if one is set in the graph settings store,
 * otherwise returns the default edge color.
 */
export function getCustomEdgeColor(edgeType: string): string {
  const custom = useGraphSettingsStore.getState().edgeColors[edgeType];
  if (custom) return custom;
  return DEFAULT_EDGE_COLOR;
}

/**
 * Returns the caption for a node based on the captionProperty setting for its label.
 * If a caption property is configured and present, uses that; otherwise falls back
 * to getNodeDisplayName.
 */
export function getNodeCaption(
  label: string,
  properties: Record<string, unknown>,
): string {
  const captionProp =
    useGraphSettingsStore.getState().captionProperty[label];
  if (captionProp) {
    const value = properties[captionProp];
    if (typeof value === "string" && value.length > 0) {
      return value;
    }
    if (value != null) {
      return String(value);
    }
  }
  return getNodeDisplayName(properties);
}
