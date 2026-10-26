export * as CopilotWebSocket from "./websocket.js"

import { InvalidResponseDataError } from "@ai-sdk/provider"
import { WebSocketTransport } from "@opencode/ai/route"
import { Cause, Effect, Exit, Schema, Stream } from "effect"
import { Headers } from "effect/http"

const decodeBody = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Record(Schema.String, Schema.Json)))
const decodeEvent = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Struct({ type: Schema.String })))
const terminal = new Set(["response.completed", "response.incomplete", "response.failed", "error"])

// Preserve the existing Copilot adapter and HTTP hooks by presenting WebSocket events as SSE.
export async function request(input: Parameters<typeof fetch>[0], init?: RequestInit) {
  const request = input instanceof Request ? new Request(input, init) : new Request(String(input), init)
  const body = decodeBody(await request.clone().text())
  if (body.stream !== true) return fetch(request)
  // HTTP hooks may have framed the POST body; the upgrade has no HTTP body.
  request.headers.delete("content-length")
  request.headers.delete("transfer-encoding")
  const url = new URL(request.url)
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:"
  const controller = new AbortController()
  const signal = AbortSignal.any([request.signal, controller.signal])
  const opened = await Effect.runPromiseExit(
    Effect.gen(function* () {
      const connection = yield* WebSocketTransport.fromWebSocket(
        new WebSocket(url, { headers: Object.fromEntries(request.headers) }),
        { url: url.href, headers: Headers.fromInput(request.headers) },
      )
      const { stream, ...fields } = body
      yield* connection
        .sendText(
          JSON.stringify({
            type: "response.create",
            ...fields,
            initiator: request.headers.get("x-initiator") ?? "user",
          }),
        )
        .pipe(Effect.onError(() => connection.close))
      return connection
    }),
    { signal },
  )
  if (Exit.isFailure(opened)) throw Cause.squash(opened.cause)
  const connection = opened.value
  const encoder = new TextEncoder()
  const decoder = new TextDecoder()
  return new Response(
    new ReadableStream<Uint8Array>({
      start(output) {
        const abort = () => output.error(signal.reason)
        signal.addEventListener("abort", abort, { once: true })
        const events = connection.messages.pipe(
          Stream.mapEffect((message) =>
            Effect.try({
              try: () => {
                const text = WebSocketTransport.messageText(message, decoder)
                return { text, type: decodeEvent(text).type }
              },
              catch: (cause) =>
                new InvalidResponseDataError({
                  message: "Invalid Copilot WebSocket response event",
                  data: { event: message, cause },
                }),
            }),
          ),
          Stream.takeUntil((event) => terminal.has(event.type)),
          Stream.runForEach((event) => Effect.sync(() => output.enqueue(encoder.encode(`data: ${event.text}\n\n`)))),
          Effect.ensuring(connection.close),
        )
        void Effect.runPromiseExit(events, { signal }).then((exit) => {
          signal.removeEventListener("abort", abort)
          if (signal.aborted) {
            output.error(signal.reason)
            return
          }
          if (Exit.isFailure(exit)) {
            output.error(Cause.squash(exit.cause))
            return
          }
          output.close()
        })
      },
      cancel(reason) {
        controller.abort(reason)
      },
    }),
    { headers: { "content-type": "text/event-stream" } },
  )
}
