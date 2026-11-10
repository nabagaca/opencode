import { expect, test } from "bun:test"
import { ai, LanguageModel, Message } from "@opencode/ai/effect"
import { LLMClient, RequestExecutor, WebSocketTransport } from "@opencode/ai/route"
import { CopilotWebSocket } from "@opencode/core/github-copilot/websocket"
import { ModelResolver } from "@opencode/core/model-resolver"
import { Model } from "@opencode/core/model"
import { Provider } from "@opencode/core/provider"
import { WebSocketConstructor } from "@opencode/core/effect/websocket-constructor"
import { Effect, Fiber, Layer, Stream } from "effect"
import { Headers } from "effect/http"
import { TestClock } from "effect/testing"
import { Socket } from "effect/socket"
import { testEffect } from "../lib/effect"

const it = testEffect(WebSocketConstructor.layer)

test.each(["user", "agent"])("declares the %s initiator in native frames", (initiator) => {
  const input =
    initiator === "user" ? [{ role: "user", content: [] }] : [{ type: "function_call_output", output: "OK" }]
  expect(JSON.parse(CopilotWebSocket.initiate(JSON.stringify({ type: "response.create", input })))).toMatchObject({
    initiator,
  })
})

it.live("streams Copilot Responses through the native parser without an HTTP request", () =>
  Effect.gen(function* () {
    const requests: unknown[] = []
    const vision: Array<string | null> = []
    const server = Bun.serve({
      port: 0,
      fetch(request, server) {
        vision.push(request.headers.get("copilot-vision-request"))
        if (server.upgrade(request)) return
        return new Response("Unexpected HTTP request", { status: 500 })
      },
      websocket: {
        message(socket, message) {
          requests.push(JSON.parse(String(message)))
          socket.send(JSON.stringify({ type: "response.created", response: { id: "resp_1" } }))
          socket.send(JSON.stringify({ type: "response.output_text.delta", item_id: "msg_1", delta: "OK" }))
          socket.send(JSON.stringify({ type: "response.completed", response: { id: "resp_1", output: [] } }))
        },
      },
    })
    yield* Effect.addFinalizer(() => Effect.promise(() => server.stop(true)))
    const model = yield* ModelResolver.fromCatalogModel({
      ...Model.Info.default(Provider.ID.githubCopilot, Model.ID.make("gpt-6.1-sol")),
      package: "@opencode/ai/providers/openai-compatible-responses",
      settings: { baseURL: server.url.origin, apiKey: "fixture" },
    })
    expect(model).toBeInstanceOf(LanguageModel)
    const constructor = yield* Socket.WebSocketConstructor
    const response = yield* LLMClient.generate(
      ai.request({
        model,
        messages: [Message.user([Message.text("Hello"), Message.media("data:image/png;base64,AAAA")])],
      }),
      {
        webSocket: WebSocketTransport.makeDirect({
          open: (input) =>
            CopilotWebSocket.open(input).pipe(Effect.provideService(Socket.WebSocketConstructor, constructor)),
        }),
      },
    ).pipe(
      Effect.provide(
        LLMClient.layer.pipe(
          Layer.provide(
            Layer.succeed(
              RequestExecutor.Service,
              RequestExecutor.Service.of({ execute: () => Effect.die("Unexpected HTTP fallback") }),
            ),
          ),
        ),
      ),
    )
    expect(response.finishReason.normalized).toBe("stop")
    expect(vision).toEqual(["true"])
    expect(requests).toEqual([expect.objectContaining({ type: "response.create", model: "gpt-6.1-sol" })])
    expect(requests[0]).not.toHaveProperty("stream")
  }),
)

it.effect("fails a socket blackholed by a proxy using the caller's clock", () =>
  Effect.gen(function* () {
    const listener = Bun.listen({
      hostname: "127.0.0.1",
      port: 0,
      socket: {
        data(socket, data) {
          const key = /sec-websocket-key: (.+)\r\n/i.exec(data.toString())?.[1]
          if (!key) return
          const accept = new Bun.CryptoHasher("sha1")
            .update(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")
            .digest("base64")
          socket.write(
            `HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${accept}\r\n\r\n`,
          )
        },
      },
    })
    yield* Effect.addFinalizer(() => Effect.sync(() => listener.stop(true)))
    const connection = yield* CopilotWebSocket.open({
      url: `ws://127.0.0.1:${listener.port}/responses`,
      headers: Headers.empty,
    })
    yield* Effect.addFinalizer(() => connection.close)
    const reading = yield* Stream.runDrain(connection.messages).pipe(
      Effect.flip,
      Effect.forkChild({ startImmediately: true }),
    )
    yield* TestClock.adjust("60 seconds")
    const error = yield* Fiber.join(reading)
    expect(error.reason).toMatchObject({ _tag: "Transport", code: "ETIMEDOUT" })
  }),
)

it.effect("keeps a silent but pong-responsive socket alive beyond the stale limit", () =>
  Effect.gen(function* () {
    const server = Bun.serve({
      port: 0,
      fetch(request, server) {
        if (server.upgrade(request)) return
        return new Response(null, { status: 400 })
      },
      websocket: {
        message(socket) {
          socket.send("done")
        },
      },
    })
    yield* Effect.addFinalizer(() => Effect.promise(() => server.stop(true)))
    const pong = { next: Promise.withResolvers<void>() }
    const connection = yield* CopilotWebSocket.open({
      url: `${server.url}responses`.replace(/^http/, "ws"),
      headers: Headers.empty,
    }).pipe(
      Effect.provideService(Socket.WebSocketConstructor, (url) => {
        const socket = new WebSocket(url)
        socket.addEventListener("pong", () => pong.next.resolve())
        return socket
      }),
    )
    yield* Effect.addFinalizer(() => connection.close)
    const reading = yield* Stream.runCollect(connection.messages.pipe(Stream.take(1))).pipe(
      Effect.forkChild({ startImmediately: true }),
    )
    for (const _ of [1, 2, 3, 4, 5]) {
      yield* TestClock.adjust("15 seconds")
      yield* Effect.promise(() => pong.next.promise)
      pong.next = Promise.withResolvers<void>()
    }
    yield* connection.sendText("finish")
    expect(Array.from(yield* Fiber.join(reading))).toEqual(["done"])
  }),
)
