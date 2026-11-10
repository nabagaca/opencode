export * as CopilotWebSocket from "./websocket.js"

import { AIError, TransportError } from "@opencode/ai/effect"
import { OpenResponsesChannel } from "@opencode/ai/protocols"
import { WebSocketTransport, type WebSocketRequest, type Transport } from "@opencode/ai/route"
import { Clock, Effect, Schema, Stream } from "effect"
import { Headers } from "effect/http"
import { Socket } from "effect/socket"

const channel = OpenResponsesChannel.transport({
  id: "github-copilot",
  name: "GitHub Copilot Responses",
  // Preserve the fork's full-history requests rather than adding response-ID continuation during the merge.
  continuation: () => undefined,
})

export const transport: Transport.Transport<unknown, OpenResponsesChannel.Prepared, string> = {
  ...channel,
  prepare: (input) =>
    channel.prepare(input).pipe(
      Effect.map((prepared) => {
        if (!prepared.channel) return prepared
        const vision = input.request.messages.some((message) =>
          message.content.some(
            (part) =>
              (part.type === "media" && part.media.kind === "image") ||
              (part.type === "tool-result" &&
                part.result.type === "content" &&
                part.result.value.some((item) => item.type === "file" && item.mime.startsWith("image/"))),
          ),
        )
        return vision
          ? {
              ...prepared,
              channel: {
                ...prepared.channel,
                headers: Headers.set(prepared.channel.headers, "Copilot-Vision-Request", "true"),
              },
            }
          : prepared
      }),
    ),
}

const frame = Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown))
const decodeFrame = Schema.decodeUnknownSync(frame)
const encodeFrame = Schema.encodeSync(frame)

export const initiate = (message: string, agent = false) => {
  const body = decodeFrame(message)
  const input = Array.isArray(body.input) ? body.input : []
  const last = input.at(-1)
  return encodeFrame({
    ...body,
    initiator:
      !agent && typeof last === "object" && last !== null && "role" in last && last.role === "user" ? "user" : "agent",
  })
}

// Keep proxy/VPN blackholes bounded without timing out a healthy peer that is still reasoning.
export const open = (input: WebSocketRequest, liveness = { pingInterval: 15_000, staleAfter: 60_000 }) =>
  Effect.gen(function* () {
    const constructor = yield* Socket.WebSocketConstructor
    const clock = yield* Clock.Clock
    const socket = constructor(input.url, { headers: input.headers })
    const connection = yield* WebSocketTransport.fromWebSocket(socket, input)
    if (!(socket instanceof WebSocket) || !("ping" in socket) || typeof socket.ping !== "function") return connection
    const ping = socket.ping.bind(socket)
    let seen = clock.currentTimeMillisUnsafe()
    const alive = () => {
      seen = clock.currentTimeMillisUnsafe()
    }
    socket.addEventListener("message", alive)
    socket.addEventListener("pong", alive)
    const monitor = Effect.gen(function* () {
      yield* Effect.sleep(liveness.pingInterval)
      if (clock.currentTimeMillisUnsafe() - seen >= liveness.staleAfter)
        return yield* new AIError({
          reason: new TransportError({
            message: `Copilot WebSocket stopped responding for ${liveness.staleAfter / 1000}s`,
            transport: "websocket",
            operation: "read",
            phase: "receive",
            code: "ETIMEDOUT",
          }),
        })
      yield* Effect.sync(() => {
        if (socket.readyState === WebSocket.OPEN) ping()
      })
    }).pipe(Effect.forever)
    return {
      ...connection,
      messages: connection.messages.pipe(Stream.merge(Stream.fromEffectDrain(monitor), { haltStrategy: "left" })),
      close: Effect.sync(() => {
        socket.removeEventListener("message", alive)
        socket.removeEventListener("pong", alive)
      }).pipe(Effect.andThen(connection.close)),
    }
  })
