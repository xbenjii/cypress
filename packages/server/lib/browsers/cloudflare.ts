import debugModule from 'debug'
import http from 'http'
import https from 'https'
import { EventEmitter } from 'events'
import WebSocket from 'ws'
import la from 'lazy-ass'
import _ from 'lodash'
import mime from 'mime'
import path from 'path'

import { CdpAutomation } from './cdp_automation'
import { BrowserCriClient } from './browser-cri-client'
import utils from './utils'
import * as errors from '../errors'
import type { Browser, BrowserInstance, GracefulShutdownOptions } from './types'
import type { CriClient } from './cri-client'
import type { Automation } from '../automation'
import type { BrowserLaunchOpts, BrowserNewTabOpts, ProtocolManagerShape, CyPromptManagerShape, StudioManagerShape } from '@packages/types'
import type { CDPSocketServer } from '@packages/socket'
import type { AddressInfo } from 'net'

const debug = debugModule('cypress:server:browsers:cloudflare')

const CLOUDFLARE_API_BASE = 'https://api.cloudflare.com/client/v4/accounts'
const KEEP_ALIVE_MS = 600000 // 10 minutes

function getCloudflareCredentials (): { accountId: string, apiToken: string } {
  const accountId = process.env.CYPRESS_CLOUDFLARE_ACCOUNT_ID

  if (!accountId) {
    throw new Error('CYPRESS_CLOUDFLARE_ACCOUNT_ID environment variable is required to use Cloudflare browser')
  }

  const apiToken = process.env.CYPRESS_CLOUDFLARE_API_TOKEN

  if (!apiToken) {
    throw new Error('CYPRESS_CLOUDFLARE_API_TOKEN environment variable is required to use Cloudflare browser')
  }

  return { accountId, apiToken }
}

function cloudflareApiRequest (method: string, url: string, apiToken: string): Promise<any> {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url)
    const options: https.RequestOptions = {
      hostname: parsed.hostname,
      path: `${parsed.pathname}${parsed.search}`,
      method,
      headers: {
        'Authorization': `Bearer ${apiToken}`,
        'Content-Type': 'application/json',
      },
    }

    const req = https.request(options, (res) => {
      let data = ''

      res.on('data', (chunk) => {
        data += chunk
      })

      res.on('end', () => {
        try {
          resolve(JSON.parse(data))
        } catch {
          resolve(data)
        }
      })
    })

    req.on('error', reject)
    req.end()
  })
}

async function createCloudflareSession (accountId: string, apiToken: string): Promise<{ sessionId: string, webSocketDebuggerUrl: string }> {
  const url = `${CLOUDFLARE_API_BASE}/${accountId}/browser-rendering/devtools/browser?keep_alive=${KEEP_ALIVE_MS}`

  debug('creating Cloudflare Browser Run session')
  const result = await cloudflareApiRequest('POST', url, apiToken)

  if (!result.sessionId) {
    throw new Error(`Failed to create Cloudflare Browser Run session: ${JSON.stringify(result)}`)
  }

  debug('Cloudflare session created: %s', result.sessionId)

  return result
}

async function closeCloudflareSession (accountId: string, apiToken: string, sessionId: string): Promise<void> {
  const url = `${CLOUDFLARE_API_BASE}/${accountId}/browser-rendering/devtools/browser/${sessionId}`

  debug('closing Cloudflare Browser Run session %s', sessionId)

  try {
    await cloudflareApiRequest('DELETE', url, apiToken)
  } catch (err) {
    debug('error closing Cloudflare session: %o', err)
  }
}

/**
 * A local HTTP/WebSocket server that proxies CDP traffic to Cloudflare Browser Run,
 * injecting the Authorization header on all outbound WebSocket connections.
 * This allows the existing CDP infrastructure (chrome-remote-interface) to connect
 * to Cloudflare without any modifications.
 */
class CloudflareCdpProxy {
  private server: http.Server | undefined
  private wss: WebSocket.Server | undefined
  private remoteConnections: Set<WebSocket> = new Set()
  // Maps local target IDs to their remote Cloudflare WebSocket URLs
  private targetWsUrls: Map<string, string> = new Map()

  constructor (
    private accountId: string,
    private apiToken: string,
    private sessionId: string,
    private cloudflareWsUrl: string,
  ) {}

  get baseApiUrl (): string {
    return `${CLOUDFLARE_API_BASE}/${this.accountId}/browser-rendering`
  }

  async start (): Promise<number> {
    this.wss = new WebSocket.Server({ noServer: true })

    this.server = http.createServer((req, res) => {
      this.handleHttpRequest(req, res)
    })

    this.server.on('upgrade', (request, socket, head) => {
      this.handleUpgrade(request, socket, head)
    })

    return new Promise<number>((resolve, reject) => {
      this.server!.listen(0, '127.0.0.1', () => {
        const port = (this.server!.address() as AddressInfo).port

        debug('Cloudflare CDP proxy listening on port %d', port)
        resolve(port)
      })

      this.server!.on('error', reject)
    })
  }

  private handleHttpRequest (req: http.IncomingMessage, res: http.ServerResponse) {
    const url = req.url || ''

    debug('proxy HTTP %s %s', req.method, url)

    if (url === '/json/version') {
      const port = (this.server!.address() as AddressInfo).port

      res.writeHead(200, { 'Content-Type': 'application/json' })
      res.end(JSON.stringify({
        Browser: 'Cloudflare Browser Run',
        'Protocol-Version': '1.3',
        'User-Agent': 'Cloudflare Browser Run',
        webSocketDebuggerUrl: `ws://127.0.0.1:${port}/devtools/browser/${this.sessionId}`,
      }))

      return
    }

    if (url === '/json' || url === '/json/list') {
      const apiUrl = `${this.baseApiUrl}/devtools/browser/${this.sessionId}/json/list`

      cloudflareApiRequest('GET', apiUrl, this.apiToken).then((targets) => {
        const port = (this.server!.address() as AddressInfo).port

        if (Array.isArray(targets)) {
          for (const target of targets) {
            if (target.webSocketDebuggerUrl) {
              this.targetWsUrls.set(target.id, target.webSocketDebuggerUrl)
              target.webSocketDebuggerUrl = `ws://127.0.0.1:${port}/devtools/page/${target.id}`
            }

            if (target.devtoolsFrontendUrl) {
              delete target.devtoolsFrontendUrl
            }
          }
        }

        res.writeHead(200, { 'Content-Type': 'application/json' })
        res.end(JSON.stringify(targets))
      }).catch((err) => {
        debug('error fetching targets: %o', err)
        res.writeHead(502)
        res.end('Failed to fetch targets from Cloudflare')
      })

      return
    }

    // For any other requests, return 404
    res.writeHead(404)
    res.end('Not Found')
  }

  private handleUpgrade (request: http.IncomingMessage, socket: any, head: Buffer) {
    const url = request.url || ''

    debug('proxy WebSocket upgrade: %s', url)

    this.wss!.handleUpgrade(request, socket, head, (localWs) => {
      let remoteUrl: string

      if (url.includes('/devtools/browser/')) {
        // Browser-level CDP connection
        remoteUrl = this.cloudflareWsUrl
      } else if (url.includes('/devtools/page/')) {
        // Page-level CDP connection
        const targetId = url.split('/devtools/page/')[1]
        const storedUrl = this.targetWsUrls.get(targetId)

        if (!storedUrl) {
          // Construct the URL if not in cache
          remoteUrl = `wss://api.cloudflare.com/client/v4/accounts/${this.accountId}/browser-rendering/devtools/browser/${this.sessionId}/page/${targetId}`
        } else {
          remoteUrl = storedUrl
        }
      } else {
        debug('unknown WebSocket path: %s', url)
        localWs.close(1008, 'Unknown path')

        return
      }

      debug('proxying WebSocket to %s', remoteUrl)

      const remoteWs = new WebSocket(remoteUrl, [], {
        headers: {
          'Authorization': `Bearer ${this.apiToken}`,
        },
      })

      this.remoteConnections.add(remoteWs)

      remoteWs.on('open', () => {
        debug('remote WebSocket connected')

        localWs.on('message', (data) => {
          if (remoteWs.readyState === WebSocket.OPEN) {
            remoteWs.send(data)
          }
        })

        remoteWs.on('message', (data) => {
          if (localWs.readyState === WebSocket.OPEN) {
            localWs.send(data)
          }
        })
      })

      localWs.on('close', (code, reason) => {
        debug('local WebSocket closed: %d %s', code, reason)
        this.remoteConnections.delete(remoteWs)

        if (remoteWs.readyState === WebSocket.OPEN) {
          remoteWs.close(code, reason)
        }
      })

      remoteWs.on('close', (code, reason) => {
        debug('remote WebSocket closed: %d %s', code, reason)
        this.remoteConnections.delete(remoteWs)

        if (localWs.readyState === WebSocket.OPEN) {
          localWs.close(code, reason)
        }
      })

      localWs.on('error', (err) => {
        debug('local WebSocket error: %o', err)
      })

      remoteWs.on('error', (err) => {
        debug('remote WebSocket error: %o', err)

        if (localWs.readyState === WebSocket.OPEN) {
          localWs.close(1011, 'Remote connection error')
        }
      })
    })
  }

  async close () {
    debug('closing Cloudflare CDP proxy')

    for (const ws of this.remoteConnections) {
      try {
        ws.close()
      } catch {
        // ignore
      }
    }

    this.remoteConnections.clear()

    return new Promise<void>((resolve) => {
      if (this.wss) {
        this.wss.close()
      }

      if (this.server) {
        this.server.close(() => resolve())
      } else {
        resolve()
      }
    })
  }
}

let browserCriClient: BrowserCriClient | undefined
let cdpProxy: CloudflareCdpProxy | undefined
let cloudflareSessionId: string | undefined
let cloudflareCredentials: { accountId: string, apiToken: string } | undefined
let onReconnect: (client: CriClient) => Promise<void> = async () => undefined

const _navigateUsingCRI = async function (client: CriClient, url: string) {
  la(_.isString(url) && url.match(/^https?:\/\/.*$/), 'missing url to navigate to', url)
  la(client, 'could not get CRI client')
  debug('navigating to page %s', url)

  await client.send('Page.bringToFront')
  await client.send('Page.navigate', { url })
}

const _handleDownloads = async function (client: CriClient, downloadsFolder: string, automation: Automation) {
  client.on('Page.downloadWillBegin', (data) => {
    const downloadItem: Record<string, any> = {
      id: data.guid,
      url: data.url,
    }

    const filename = data.suggestedFilename

    if (filename) {
      downloadItem.filePath = path.join(downloadsFolder, data.suggestedFilename)
      downloadItem.mime = mime.getType(data.suggestedFilename)
    }

    automation.push('create:download', downloadItem)
  })

  client.on('Page.downloadProgress', (data) => {
    if (data.state === 'completed') {
      automation.push('complete:download', {
        id: data.guid,
      })
    }

    if (data.state === 'canceled') {
      automation.push('canceled:download', {
        id: data.guid,
      })
    }
  })

  await client.send('Page.setDownloadBehavior', {
    behavior: 'allow',
    downloadPath: downloadsFolder,
  })
}

const _setAutomation = async (client: CriClient, automation: Automation, resetBrowserTargets: (shouldKeepTabOpen: boolean) => Promise<void>, options: BrowserLaunchOpts) => {
  const cdpAutomation = await CdpAutomation.create(client.send, client.on, client.off, resetBrowserTargets, automation, options.protocolManager, false, true)

  automation.use(cdpAutomation)

  return cdpAutomation
}

export = {
  _navigateUsingCRI,

  _handleDownloads,

  _setAutomation,

  _getBrowserCriClient () {
    return browserCriClient
  },

  clearInstanceState (options: GracefulShutdownOptions = {}) {
    debug('closing remote interface client', { options })
    browserCriClient?.close(options.gracefulShutdown).catch(() => {})
    browserCriClient = undefined

    if (cloudflareSessionId && cloudflareCredentials) {
      closeCloudflareSession(cloudflareCredentials.accountId, cloudflareCredentials.apiToken, cloudflareSessionId).catch(() => {})
      cloudflareSessionId = undefined
    }

    if (cdpProxy) {
      cdpProxy.close().catch(() => {})
      cdpProxy = undefined
    }
  },

  async connectProtocolToBrowser (options: { protocolManager?: ProtocolManagerShape }) {
    const browserCriClient = this._getBrowserCriClient()

    if (!browserCriClient?.currentlyAttachedTarget) throw new Error('Missing pageCriClient in connectProtocolToBrowser')

    if (!browserCriClient.currentlyAttachedProtocolTarget) {
      browserCriClient.currentlyAttachedProtocolTarget = await browserCriClient.currentlyAttachedTarget.clone()
    }

    await options.protocolManager?.connectToBrowser(browserCriClient.currentlyAttachedProtocolTarget)
  },

  async connectCyPromptToBrowser (options: { cyPromptManager?: CyPromptManagerShape }) {
    const browserCriClient = this._getBrowserCriClient()

    if (!browserCriClient?.currentlyAttachedTarget) throw new Error('Missing pageCriClient in connectCyPromptToBrowser')

    if (!browserCriClient.currentlyAttachedCyPromptTarget) {
      browserCriClient.currentlyAttachedCyPromptTarget = await browserCriClient.currentlyAttachedTarget.clone()
    }

    await options.cyPromptManager?.connectToBrowser(browserCriClient.currentlyAttachedCyPromptTarget)
  },

  async connectStudioToBrowser (options: { studioManager?: StudioManagerShape }) {
    const browserCriClient = this._getBrowserCriClient()

    if (!browserCriClient?.currentlyAttachedTarget) throw new Error('Missing pageCriClient in connectStudioToBrowser')

    if (!browserCriClient.currentlyAttachedStudioTarget) {
      browserCriClient.currentlyAttachedStudioTarget = await browserCriClient.currentlyAttachedTarget.clone()
    }

    await options.studioManager?.connectToBrowser(browserCriClient.currentlyAttachedStudioTarget)
  },

  async closeProtocolConnection () {
    const browserCriClient = this._getBrowserCriClient()

    if (browserCriClient?.currentlyAttachedProtocolTarget) {
      await browserCriClient.currentlyAttachedProtocolTarget.close()
      browserCriClient.currentlyAttachedProtocolTarget = undefined
    }
  },

  async connectToNewSpec (browser: Browser, options: BrowserNewTabOpts, automation: Automation, socketServer?: CDPSocketServer) {
    debug('connecting to new Cloudflare tab in existing instance with url %s', options.url)

    const browserCriClient = this._getBrowserCriClient()

    if (!browserCriClient) throw new Error('Missing browserCriClient in connectToNewSpec')

    const pageCriClient = browserCriClient.currentlyAttachedTarget

    if (!pageCriClient) throw new Error('Missing pageCriClient in connectToNewSpec')

    if (!options.url) throw new Error('Missing url in connectToNewSpec')

    await this.connectProtocolToBrowser({ protocolManager: options.protocolManager })
    await socketServer?.attachCDPClient(pageCriClient)

    await this.attachListeners(options.url, pageCriClient, automation, options, browser)
  },

  async connectToExisting (browser: Browser, options: BrowserLaunchOpts, automation: Automation, cdpSocketServer?: CDPSocketServer) {
    // For Cloudflare, connecting to existing means re-using the current proxy
    const browserCriClient = this._getBrowserCriClient()

    if (!browserCriClient) throw new Error('Missing browserCriClient in connectToExisting')

    if (!options.url) throw new Error('Missing url in connectToExisting')

    const pageCriClient = await browserCriClient.attachToTargetUrl(options.url)

    await cdpSocketServer?.attachCDPClient(pageCriClient)

    await this._setAutomation(pageCriClient, automation, browserCriClient.resetBrowserTargets, options)
  },

  async attachListeners (url: string, pageCriClient: CriClient, automation: Automation, options: BrowserLaunchOpts | BrowserNewTabOpts, browser: Browser) {
    const browserCriClient = this._getBrowserCriClient()

    debug('attaching crash handler to target ', pageCriClient.targetId)
    pageCriClient.on('Target.targetCrashed', async (event) => {
      debug('target crashed!', event)

      if (event.targetId !== browserCriClient?.currentlyAttachedTarget?.targetId) {
        return
      }

      const err = errors.get('RENDERER_CRASHED', browser.displayName)

      if (!options.onError) {
        errors.log(err)
        throw new Error('Missing onError in attachListeners')
      }

      options.onError(err)
    })

    if (!browserCriClient) throw new Error('Missing browserCriClient in attachListeners')

    debug('attaching listeners to Cloudflare browser %o', { url, options: _.omit(options, 'browsers') })

    const cdpAutomation = await this._setAutomation(pageCriClient, automation, browserCriClient.resetBrowserTargets, options)

    onReconnect = (client: CriClient) => {
      // @ts-expect-error
      return cdpAutomation._updateFrameTree(client, 'onReconnect')()
    }

    await pageCriClient.send('Page.enable')

    await options['onInitializeNewBrowserTab']?.()

    await Promise.all([
      pageCriClient.send('ServiceWorker.enable'),
      this._handleDownloads(pageCriClient, options.downloadsFolder, automation),
      utils.initializeCDP(pageCriClient, automation),
    ])

    await this._navigateUsingCRI(pageCriClient, url)

    await cdpAutomation._handlePausedRequests(pageCriClient)
    cdpAutomation._listenForFrameTreeChanges(pageCriClient)

    return cdpAutomation
  },

  async open (browser: Browser, url: string, options: BrowserLaunchOpts, automation: Automation, cdpSocketServer?: CDPSocketServer): Promise<BrowserInstance> {
    const credentials = getCloudflareCredentials()

    cloudflareCredentials = credentials

    // 1. Create a Cloudflare Browser Run session
    const session = await createCloudflareSession(credentials.accountId, credentials.apiToken)

    cloudflareSessionId = session.sessionId

    // 2. Start local CDP proxy that adds auth headers
    cdpProxy = new CloudflareCdpProxy(
      credentials.accountId,
      credentials.apiToken,
      session.sessionId,
      session.webSocketDebuggerUrl,
    )

    const proxyPort = await cdpProxy.start()

    debug('CDP proxy started on port %d, connecting to Cloudflare session %s', proxyPort, session.sessionId)

    // 3. Create a virtual BrowserInstance (no local process)
    const instance = new EventEmitter() as BrowserInstance

    instance.pid = 0
    instance.kill = () => {
      this.clearInstanceState({ gracefulShutdown: true })
      debug('closing Cloudflare browser')
      instance.emit('exit', 0, null)
    }

    // 4. Connect via CDP through the local proxy
    if (!options.onError) throw new Error('Missing onError in cloudflare#open')

    browserCriClient = await BrowserCriClient.create({
      hosts: ['127.0.0.1'],
      port: proxyPort,
      browserName: browser.displayName,
      onAsynchronousError: options.onError,
      onReconnect,
      protocolManager: options.protocolManager,
      fullyManageTabs: true,
      onServiceWorkerClientEvent: automation.onServiceWorkerClientEvent,
    })

    la(browserCriClient, 'expected Chrome remote interface reference', browserCriClient)

    // 5. Monkey-patch kill to close CDP first
    const originalKill = instance.kill

    ;(instance as any).browserCriClient = browserCriClient

    instance.kill = (...args) => {
      originalKill.apply(instance, args)
    }

    // 6. Navigate to about:blank first, then set up automation
    const pageCriClient = await browserCriClient.attachToTargetUrl('about:blank')

    await cdpSocketServer?.attachCDPClient(pageCriClient)

    await this.attachListeners(url, pageCriClient, automation, options, browser)

    await utils.executeAfterBrowserLaunch(browser, {
      webSocketDebuggerUrl: browserCriClient.getWebSocketDebuggerUrl(),
    })

    return instance
  },

  async closeExtraTargets () {
    return browserCriClient?.closeExtraTargets()
  },
}
