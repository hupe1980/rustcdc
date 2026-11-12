// Server-sent events read through `fetch`, because `EventSource` cannot send the bearer token.
// The parser is separate from the reading so chunk boundaries can be tested: a frame may arrive in
// any number of pieces, and only a blank line ends it.
export type Frame = { event: string; data: string; id?: string };

export function parser(onframe: (frame: Frame) => void): (chunk: string) => void {
  let buffer = "";
  return (chunk) => {
    buffer += chunk.replace(/\r\n?/g, "\n");
    let end: number;
    while ((end = buffer.indexOf("\n\n")) >= 0) {
      const raw = buffer.slice(0, end);
      buffer = buffer.slice(end + 2);
      const frame: Frame = { event: "message", data: "" };
      const data: string[] = [];
      for (const line of raw.split("\n")) {
        if (!line || line.startsWith(":")) continue;
        const colon = line.indexOf(":");
        const field = colon < 0 ? line : line.slice(0, colon);
        const value = colon < 0 ? "" : line.slice(colon + 1).replace(/^ /, "");
        if (field === "event") frame.event = value;
        else if (field === "data") data.push(value);
        else if (field === "id") frame.id = value;
      }
      if (data.length) onframe({ ...frame, data: data.join("\n") });
    }
  };
}

/** Read a stream until it ends or `signal` aborts. */
export async function read(body: ReadableStream<Uint8Array>, onframe: (frame: Frame) => void, signal: AbortSignal) {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  const feed = parser(onframe);
  signal.addEventListener("abort", () => void reader.cancel());
  for (;;) {
    const { value, done } = await reader.read();
    if (done || signal.aborted) return;
    feed(decoder.decode(value, { stream: true }));
  }
}
