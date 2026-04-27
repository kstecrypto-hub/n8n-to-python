import type { ReactNode } from "react";
import type { AdminExtendedSection } from "@/features/admin/adminModels";

export interface AdminExtendedSectionProps {
  section: AdminExtendedSection;
  usingToken: boolean;
  permissions: Set<string>;
  tenantId: string;
}

export function pretty(value: unknown) {
  return JSON.stringify(value ?? {}, null, 2);
}

export function sid(value: unknown) {
  const text = String(value ?? "").trim();
  return text.length > 18 ? `${text.slice(0, 8)}...${text.slice(-4)}` : text || "n/a";
}

export function can(props: AdminExtendedSectionProps, permission: string) {
  return props.usingToken || props.permissions.has(permission);
}

export function asRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : null;
}

export function asArray(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

export function memoryCount(value: unknown): number {
  if (Array.isArray(value)) return value.length;
  return String(value ?? "").trim() ? 1 : 0;
}

export function memoryPreview(value: unknown): string {
  if (Array.isArray(value)) {
    const first = value[0];
    if (!first) return "empty";
    if (typeof first === "string") return first;
    const record = asRecord(first);
    if (!record) return String(first);
    return String(
      record.fact ??
        record.thread ??
        record.preference ??
        record.constraint ??
        record.topic ??
        record.goal ??
        JSON.stringify(record),
    );
  }
  return String(value ?? "").trim() || "empty";
}

export function renderMemoryItems(value: unknown, kind: "facts" | "threads" | "generic"): ReactNode {
  const items = asArray(value).slice(0, 6);
  if (!items.length) return <div className="muted">No items.</div>;
  return (
    <ul className="admin-list admin-list--compact">
      {items.map((item, index) => {
        const record = asRecord(item);
        const label =
          String(
            record?.fact ??
              record?.thread ??
              record?.preference ??
              record?.constraint ??
              record?.topic ??
              record?.goal ??
              item,
          ) || "item";
        const meta: string[] = [];
        if (kind === "facts") {
          const source = String(record?.source_type ?? "");
          const confidence = record?.confidence;
          const reviewPolicy = String(record?.review_policy ?? "");
          if (source) meta.push(source);
          if (typeof confidence === "number") meta.push(`confidence ${confidence.toFixed(2)}`);
          if (reviewPolicy) meta.push(reviewPolicy);
        }
        if (kind === "threads") {
          const source = String(record?.source ?? "");
          const expiry = String(record?.expiry_policy ?? "");
          const questionType = String(record?.question_type ?? "");
          if (source) meta.push(source);
          if (questionType) meta.push(questionType);
          if (expiry) meta.push(expiry);
        }
        if (kind === "generic") {
          const source = String(record?.source ?? "");
          if (source) meta.push(source);
        }
        return (
          <li key={`${label}-${index}`} className="admin-list-item">
            <strong>{label}</strong>
            {meta.length ? <div className="muted">{meta.join(" | ")}</div> : null}
          </li>
        );
      })}
    </ul>
  );
}
