export class ApiError extends Error {
  readonly status: number;
  readonly body: unknown;

  constructor(message: string, status: number, body: unknown) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.body = body;
  }
}

type Primitive = string | number | boolean;

export interface RequestJsonOptions extends Omit<RequestInit, "body"> {
  body?: unknown;
}

export function buildQuery(params: Record<string, Primitive | Primitive[] | null | undefined>): string {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === null || value === undefined) {
      continue;
    }
    if (Array.isArray(value)) {
      for (const item of value) {
        query.append(key, String(item));
      }
      continue;
    }
    query.set(key, String(value));
  }
  const text = query.toString();
  return text ? `?${text}` : "";
}

async function readResponseBody(response: Response): Promise<unknown> {
  if (response.status === 204) {
    return null;
  }
  const text = await response.text();
  if (!text) {
    return null;
  }
  try {
    return JSON.parse(text) as unknown;
  } catch {
    return text;
  }
}

export async function requestJson<T>(path: string, options: RequestJsonOptions = {}): Promise<T> {
  const { body, ...rest } = options;
  const headers = new Headers(options.headers);
  headers.set("Accept", "application/json");
  const init: RequestInit = {
    credentials: "include",
    ...rest,
    headers,
  };

  if (body !== undefined) {
    if (body instanceof FormData) {
      init.body = body;
    } else {
      headers.set("Content-Type", "application/json");
      init.body = JSON.stringify(body);
    }
  }

  const response = await fetch(path, init);
  const responseBody = await readResponseBody(response);
  if (!response.ok) {
    const detail =
      typeof responseBody === "object" && responseBody && "detail" in responseBody
        ? String((responseBody as { detail?: unknown }).detail ?? response.statusText)
        : response.statusText;
    throw new ApiError(detail || "Request failed", response.status, responseBody);
  }
  return responseBody as T;
}
