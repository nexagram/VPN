// === VLESS over WebSocket — Cloudflare Worker ===
// Разверните этот файл как Cloudflare Worker.
// Перед деплоем замените userID на свой собственный UUID.

// Сгенерируйте свой UUID командой: 
//   node -e "console.log(crypto.randomUUID())"
// или на сайте uuidgenerator.net, и вставьте сюда:
const userID = '8b7958c0-4e2e-4046-8e9a-9ba35dd859c4';

export default {
  async fetch(request) {
    try {
      const upgradeHeader = request.headers.get('Upgrade');
      if (!upgradeHeader || upgradeHeader !== 'websocket') {
        // Обычный HTTP-запрос — отдаём диагностическую страницу:
        // пробуем загрузить cloudflare:sockets прямо тут, чтобы
        // ошибка (если она есть) была видна в браузере.
        try {
          await import('cloudflare:sockets');
          return new Response('OK: sockets module loaded fine', { status: 200 });
        } catch (e) {
          return new Response('DIAGNOSTIC ERROR loading cloudflare:sockets: ' + e.toString(), { status: 200 });
        }
      }
      return await vlessOverWSHandler(request);
    } catch (err) {
      return new Response('FETCH ERROR: ' + err.toString() + '\n' + (err.stack || ''));
    }
  },
};

async function vlessOverWSHandler(request) {
  const webSocketPair = new WebSocketPair();
  const [client, webSocket] = Object.values(webSocketPair);
  webSocket.accept();

  let remoteSocketWrapper = { value: null };
  let udpStreamWrite = null;
  let isDns = false;

  const readableStream = makeReadableWebSocketStream(webSocket);

  readableStream
    .pipeTo(
      new WritableStream({
        async write(chunk) {
          if (isDns && udpStreamWrite) {
            return udpStreamWrite(chunk);
          }
          if (remoteSocketWrapper.value) {
            const writer = remoteSocketWrapper.value.writable.getWriter();
            await writer.write(chunk);
            writer.releaseLock();
            return;
          }

          const {
            hasError,
            message,
            addressRemote,
            portRemote,
            rawDataIndex,
            vlessVersion,
          } = processVlessHeader(chunk, userID);

          if (hasError) {
            throw new Error(message);
          }

          const vlessResponseHeader = new Uint8Array([vlessVersion[0], 0]);
          const rawClientData = chunk.slice(rawDataIndex);

          await handleTCPOutBound(
            remoteSocketWrapper,
            addressRemote,
            portRemote,
            rawClientData,
            webSocket,
            vlessResponseHeader
          );
        },
      })
    )
    .catch(() => {});

  return new Response(null, { status: 101, webSocket: client });
}

function makeReadableWebSocketStream(webSocketServer) {
  let readableStreamCancel = false;
  return new ReadableStream({
    start(controller) {
      webSocketServer.addEventListener('message', (event) => {
        if (readableStreamCancel) return;
        controller.enqueue(event.data);
      });
      webSocketServer.addEventListener('close', () => {
        if (readableStreamCancel) return;
        controller.close();
      });
      webSocketServer.addEventListener('error', (err) => {
        controller.error(err);
      });
    },
    cancel() {
      readableStreamCancel = true;
      webSocketServer.close();
    },
  });
}

function processVlessHeader(vlessBuffer, userIDStr) {
  if (vlessBuffer.byteLength < 24) {
    return { hasError: true, message: 'invalid data' };
  }
  const version = new Uint8Array(vlessBuffer.slice(0, 1));
  let isValidUser = false;
  const slicedBufferString = stringify(new Uint8Array(vlessBuffer.slice(1, 17)));
  if (slicedBufferString === userIDStr) isValidUser = true;
  if (!isValidUser) {
    return { hasError: true, message: 'invalid user' };
  }

  const optLength = new Uint8Array(vlessBuffer.slice(17, 18))[0];
  const command = new Uint8Array(
    vlessBuffer.slice(18 + optLength, 18 + optLength + 1)
  )[0];

  if (command !== 1) {
    return { hasError: true, message: `command ${command} is not supported, only TCP (1)` };
  }

  const portIndex = 18 + optLength + 1;
  const portBuffer = vlessBuffer.slice(portIndex, portIndex + 2);
  const portRemote = new DataView(portBuffer).getUint16(0);

  let addressIndex = portIndex + 2;
  const addressBuffer = new Uint8Array(
    vlessBuffer.slice(addressIndex, addressIndex + 1)
  );
  const addressType = addressBuffer[0];
  let addressLength = 0;
  let addressValueIndex = addressIndex + 1;
  let addressValue = '';

  switch (addressType) {
    case 1: // IPv4
      addressLength = 4;
      addressValue = new Uint8Array(
        vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength)
      ).join('.');
      break;
    case 2: // domain
      addressLength = new Uint8Array(
        vlessBuffer.slice(addressValueIndex, addressValueIndex + 1)
      )[0];
      addressValueIndex += 1;
      addressValue = new TextDecoder().decode(
        vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength)
      );
      break;
    case 3: // IPv6
      addressLength = 16;
      const dataView = new DataView(
        vlessBuffer.slice(addressValueIndex, addressValueIndex + addressLength)
      );
      const ipv6 = [];
      for (let i = 0; i < 8; i++) ipv6.push(dataView.getUint16(i * 2).toString(16));
      addressValue = ipv6.join(':');
      break;
    default:
      return { hasError: true, message: `invalid addressType: ${addressType}` };
  }

  return {
    hasError: false,
    addressRemote: addressValue,
    portRemote,
    rawDataIndex: addressValueIndex + addressLength,
    vlessVersion: version,
  };
}

async function handleTCPOutBound(
  remoteSocket,
  addressRemote,
  portRemote,
  rawClientData,
  webSocket,
  vlessResponseHeader
) {
  async function connectAndWrite(address, port) {
    const { connect } = await import('cloudflare:sockets');
    const tcpSocket = connect({ hostname: address, port });
    remoteSocket.value = tcpSocket;
    const writer = tcpSocket.writable.getWriter();
    await writer.write(rawClientData);
    writer.releaseLock();
    console.log(`[connect] wrote initial data to ${address}:${port}`);
    return tcpSocket;
  }

  let tcpSocket;
  try {
    tcpSocket = await connectAndWrite(addressRemote, portRemote);
  } catch (err) {
    console.log(`[connect] FAILED ${addressRemote}:${portRemote} — ${err}`);
    try { webSocket.close(1011, 'upstream connect failed'); } catch (e) {}
    return;
  }

  tcpSocket.readable
    .pipeTo(
      new WritableStream({
        async write(chunk) {
          if (webSocket.readyState !== 1) return;
          let dataToSend = chunk;
          if (vlessResponseHeader) {
            const combined = new Uint8Array(vlessResponseHeader.byteLength + chunk.byteLength);
            combined.set(vlessResponseHeader, 0);
            combined.set(new Uint8Array(chunk), vlessResponseHeader.byteLength);
            dataToSend = combined;
            vlessResponseHeader = null;
          }
          webSocket.send(dataToSend);
        },
      })
    )
    .catch((err) => {
      console.log(`[pipe] remote->client error: ${err}`);
      try { webSocket.close(); } catch (e) {}
    });
}

function stringify(arr) {
  const byteToHex = [];
  for (let i = 0; i < 256; ++i) {
    byteToHex.push((i + 256).toString(16).slice(1));
  }
  const segments = [4, 2, 2, 2, 6];
  let offset = 0;
  const parts = [];
  for (const len of segments) {
    let s = '';
    for (let i = 0; i < len; i++) s += byteToHex[arr[offset + i]];
    parts.push(s);
    offset += len;
  }
  return `${parts[0]}-${parts[1]}-${parts[2]}-${parts[3]}-${parts[4]}`;
}
