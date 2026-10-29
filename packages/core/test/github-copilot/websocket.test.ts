import { expect, test } from "bun:test"
import { CopilotWebSocket } from "@opencode/core/github-copilot/websocket"
import { InvalidResponseDataError } from "@ai-sdk/provider"
import { AIError } from "@opencode/ai/effect"

function server(messages: string[], close = false) {
  const closed = Promise.withResolvers<void>()
  const requests: unknown[] = []
  const headers: Headers[] = []
  const instance = Bun.serve({
    port: 0,
    fetch(request, server) {
      headers.push(request.headers)
      if (server.upgrade(request)) return
      return Response.json({ ordinary: true })
    },
    websocket: {
      message(socket, message) {
        requests.push(JSON.parse(String(message)))
        messages.forEach((message) => socket.send(message))
        if (close) socket.close()
      },
      close() {
        closed.resolve()
      },
    },
  })
  return {
    instance,
    closed: closed.promise,
    requests,
    headers,
    request: (signal?: AbortSignal) =>
      CopilotWebSocket.request(`${instance.url}responses`, {
        method: "POST",
        headers: { authorization: "Bearer fixture", "content-type": "application/json", "x-initiator": "agent" },
        body: JSON.stringify({ model: "gpt-6.1-sol", stream: true, store: false, tools: [] }),
        signal,
      }),
  }
}

test("streams complete provider events and closes the connection", async () => {
  const fixture = server([
    '{"type":"response.output_text.delta","delta":"OK"}',
    '{"type":"response.completed","response":{"status":"completed"}}',
  ])
  try {
    const response = await fixture.request()
    expect(await response.text()).toBe(
      'data: {"type":"response.output_text.delta","delta":"OK"}\n\n' +
        'data: {"type":"response.completed","response":{"status":"completed"}}\n\n',
    )
    expect(fixture.requests).toEqual([
      {
        type: "response.create",
        model: "gpt-6.1-sol",
        store: false,
        tools: [],
        initiator: "agent",
      },
    ])
    expect(fixture.headers[0]?.get("authorization")).toBe("Bearer fixture")
    await fixture.closed
  } finally {
    await fixture.instance.stop(true)
  }
})

test.each(["response.incomplete", "response.failed", "error"])(
  "ends on %s without fabricating completion",
  async (type) => {
    const fixture = server([JSON.stringify({ type })])
    try {
      expect(await (await fixture.request()).text()).toBe(`data: ${JSON.stringify({ type })}\n\n`)
      await fixture.closed
    } finally {
      await fixture.instance.stop(true)
    }
  },
)

test("reports early socket closure as a transport error", async () => {
  const fixture = server([], true)
  try {
    await expect(fixture.request().then((response) => response.text())).rejects.toBeInstanceOf(AIError)
    await fixture.closed
  } finally {
    await fixture.instance.stop(true)
  }
})

test("reports malformed JSON as invalid provider output", async () => {
  const fixture = server(['{"type":"response.completed"'])
  try {
    await expect(fixture.request().then((response) => response.text())).rejects.toBeInstanceOf(InvalidResponseDataError)
    await fixture.closed
  } finally {
    await fixture.instance.stop(true)
  }
})

test.each(["abort", "cancel"])("%s closes the active socket", async (mode) => {
  const fixture = server([])
  const controller = new AbortController()
  try {
    const response = await fixture.request(controller.signal)
    if (mode === "abort") {
      const text = response.text()
      controller.abort()
      await expect(text).rejects.toMatchObject({ name: "AbortError" })
    }
    if (mode === "cancel") await response.body?.cancel()
    await fixture.closed
  } finally {
    await fixture.instance.stop(true)
  }
})

test("keeps non-streaming generation on HTTP", async () => {
  const fixture = server([])
  try {
    const response = await CopilotWebSocket.request(`${fixture.instance.url}responses`, {
      method: "POST",
      body: JSON.stringify({ model: "gpt-6.1-sol", stream: false }),
    })
    expect(await response.json()).toEqual({ ordinary: true })
    expect(fixture.requests).toEqual([])
  } finally {
    await fixture.instance.stop(true)
  }
})

const body = JSON.stringify({ model: "gpt-6.1-sol", stream: true })
const quick = CopilotWebSocket.make({ pingInterval: 20, staleAfter: 150 })

test("keeps a silent stream alive while the peer answers pings", async () => {
  const instance = Bun.serve({
    port: 0,
    fetch(request, server) {
      if (server.upgrade(request)) return
      return new Response(null, { status: 400 })
    },
    websocket: {
      message(socket) {
        setTimeout(() => socket.send('{"type":"response.completed"}'), 400)
      },
    },
  })
  try {
    const response = await quick(`${instance.url}responses`, { method: "POST", body })
    expect(await response.text()).toBe('data: {"type":"response.completed"}\n\n')
  } finally {
    await instance.stop(true)
  }
})

test("fails a stream whose peer stops responding", async () => {
  // A raw socket completes the upgrade, then never answers frames or pings, like a connection dropped by a proxy.
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
  try {
    const response = await quick(`http://127.0.0.1:${listener.port}/responses`, { method: "POST", body })
    await expect(response.text()).rejects.toMatchObject({ code: "ETIMEDOUT" })
  } finally {
    listener.stop(true)
  }
})
