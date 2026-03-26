"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.makeSocket = void 0;
const boom_1 = require("@hapi/boom");
const crypto_1 = require("crypto");
const url_1 = require("url");
const util_1 = require("util");
const WAProto_1 = require("../../WAProto");
const Defaults_1 = require("../Defaults");
const Types_1 = require("../Types");
const Utils_1 = require("../Utils");
const WABinary_1 = require("../WABinary");
const Client_1 = require("./Client");

/**
 * Connects to WA servers and performs:
 * - simple queries (no retry mechanism, wait for connection establishment)
 * - listen to messages and emit events
 * - query phone connection
 */
const makeSocket = (config) => {
    var _a, _b;
    const { waWebSocketUrl, connectTimeoutMs, logger, keepAliveIntervalMs, browser, auth: authState, printQRInTerminal, defaultQueryTimeoutMs, transactionOpts, qrTimeout, makeSignalRepository, } = config;
    
    // Enhanced config with retry options
    const retryConfig = {
        maxRetries: 3,
        initialDelay: 1000,
        maxDelay: 10000,
        ...config.retryConfig
    };

    const url = typeof waWebSocketUrl === 'string' ? new url_1.URL(waWebSocketUrl) : waWebSocketUrl;
    
    if (config.mobile || url.protocol === 'tcp:') {
        throw new boom_1.Boom('Mobile API is not supported anymore', { 
            statusCode: Types_1.DisconnectReason.loggedOut 
        });
    }
    
    if (url.protocol === 'wss' && ((_a = authState === null || authState === void 0 ? void 0 : authState.creds) === null || _a === void 0 ? void 0 : _a.routingInfo)) {
        url.searchParams.append('ED', authState.creds.routingInfo.toString('base64url'));
    }

    const ws = new Client_1.WebSocketClient(url, config);
    ws.connect();

    const ev = (0, Utils_1.makeEventBuffer)(logger);
    
    /** ephemeral key pair used to encrypt/decrypt communication. Unique for each connection */
    const ephemeralKeyPair = Utils_1.Curve.generateKeyPair();
    
    /** WA noise protocol wrapper */
    const noise = (0, Utils_1.makeNoiseHandler)({
        keyPair: ephemeralKeyPair,
        NOISE_HEADER: Defaults_1.NOISE_WA_HEADER,
        logger,
        routingInfo: (_b = authState === null || authState === void 0 ? void 0 : authState.creds) === null || _b === void 0 ? void 0 : _b.routingInfo
    });

    const { creds } = authState;
    
    // add transaction capability
    const keys = (0, Utils_1.addTransactionCapability)(authState.keys, logger, transactionOpts);
    const signalRepository = makeSignalRepository({ creds, keys });

    let lastDateRecv;
    let epoch = 1;
    let keepAliveReq;
    let qrTimer;
    let closed = false;
    let isReconnecting = false;
    let connectionAttempts = 0;
    const maxConnectionAttempts = 5;
    
    const uqTagId = (0, Utils_1.generateMdTagPrefix)();
    const generateMessageTag = () => `${uqTagId}${epoch++}`;

    const sendPromise = (0, util_1.promisify)(ws.send);

    /** Exponential backoff delay */
    const getRetryDelay = (attempt) => {
        const delay = retryConfig.initialDelay * Math.pow(2, attempt - 1);
        return Math.min(delay, retryConfig.maxDelay);
    };

    /** send a raw buffer with retry mechanism */
    const sendRawMessage = async (data, attempt = 1) => {
        if (!ws.isOpen) {
            throw new boom_1.Boom('Connection Closed', { statusCode: Types_1.DisconnectReason.connectionClosed });
        }

        const bytes = noise.encodeFrame(data);
        
        try {
            await (0, Utils_1.promiseTimeout)(connectTimeoutMs, async (resolve, reject) => {
                try {
                    await sendPromise.call(ws, bytes);
                    resolve();
                } catch (error) {
                    reject(error);
                }
            });
        } catch (error) {
            if (attempt <= retryConfig.maxRetries && 
                (error.message.includes('timeout') || 
                 error.message.includes('ECONNRESET') ||
                 error.message.includes('socket'))) {
                
                logger.warn({ attempt, error: error.message }, `Retrying sendRawMessage (${attempt}/${retryConfig.maxRetries})`);
                await (0, Utils_1.delay)(getRetryDelay(attempt));
                return sendRawMessage(data, attempt + 1);
            }
            throw error;
        }
    };

    /** send a binary node */
    const sendNode = (frame) => {
        if (logger.level === 'trace') {
            logger.trace({ xml: (0, WABinary_1.binaryNodeToString)(frame), msg: 'xml send' });
        }
        const buff = (0, WABinary_1.encodeBinaryNode)(frame);
        return sendRawMessage(buff);
    };

    /** Safe send node with error handling */
    const safeSendNode = async (node) => {
        try {
            return await sendNode(node);
        } catch (error) {
            logger.error({ error: error.message }, 'Failed to send node');
            onUnexpectedError(error, 'safeSendNode');
            throw error;
        }
    };

    /** log & process any unexpected errors */
    const onUnexpectedError = (err, msg) => {
        logger.error({ err, context: msg }, `unexpected error in '${msg}'`);
        const message = (err && ((err.stack || err.message) || String(err))).toLowerCase();
        
        // auto recover from cryptographic desyncs by re-uploading prekeys
        if (message.includes('bad mac') || (message.includes('mac') && message.includes('invalid'))) {
            logger.warn('Detected cryptographic desync, attempting recovery...');
            try {
                uploadPreKeysToServerIfRequired(true)
                    .catch(e => logger.warn({ e }, 'failed to re-upload prekeys after bad mac'));
            } catch (_e) {
                // ignore
            }
        }
        
        // gently back off when encountering rate limits (429)
        if (message.includes('429') || message.includes('rate limit')) {
            const wait = Math.min(30000, (config.backoffDelayMs || 10000));
            logger.info({ wait }, 'backing off due to rate limit');
            setTimeout(() => {
                // recovery after rate limit
                if (ws.isOpen) {
                    logger.info('Recovering from rate limit');
                }
            }, wait);
        }
        
        // Handle connection errors
        if (message.includes('econnreset') || message.includes('socket') || message.includes('connection')) {
            if (!isReconnecting && !closed) {
                logger.info('Attempting to recover from connection error');
                attemptGracefulRecovery();
            }
        }
    };

    /** Graceful recovery attempt */
    const attemptGracefulRecovery = async () => {
        if (isReconnecting || closed) return;
        
        isReconnecting = true;
        connectionAttempts++;
        
        if (connectionAttempts > maxConnectionAttempts) {
            logger.error('Max reconnection attempts reached');
            end(new boom_1.Boom('Max reconnection attempts reached', { 
                statusCode: Types_1.DisconnectReason.connectionLost 
            }));
            return;
        }
        
        try {
            logger.info({ attempt: connectionAttempts }, 'Attempting connection recovery');
            
            // Reset state
            clearInterval(keepAliveReq);
            clearTimeout(qrTimer);
            
            // Attempt to re-establish connection
            await waitForSocketOpen();
            
            // Re-authenticate if needed
            if (!creds.me) {
                await validateConnection();
            } else {
                // Send passive IQ to verify connection
                await sendPassiveIq('active');
            }
            
            // Reset attempts on successful recovery
            connectionAttempts = 0;
            isReconnecting = false;
            logger.info('Connection recovered successfully');
            
        } catch (error) {
            logger.error({ error: error.message, attempt: connectionAttempts }, 'Recovery attempt failed');
            
            // Exponential backoff before next attempt
            const delay = getRetryDelay(connectionAttempts);
            logger.info({ delay }, 'Waiting before next recovery attempt');
            
            setTimeout(() => {
                isReconnecting = false;
                attemptGracefulRecovery();
            }, delay);
        }
    };

    /** await the next incoming message */
    const awaitNextMessage = async (sendMsg) => {
        if (!ws.isOpen) {
            throw new boom_1.Boom('Connection Closed', {
                statusCode: Types_1.DisconnectReason.connectionClosed
            });
        }

        let onOpen;
        let onClose;
        
        const result = (0, Utils_1.promiseTimeout)(connectTimeoutMs, (resolve, reject) => {
            onOpen = resolve;
            onClose = mapWebSocketError(reject);
            ws.on('frame', onOpen);
            ws.on('close', onClose);
            ws.on('error', onClose);
        })
        .finally(() => {
            ws.off('frame', onOpen);
            ws.off('close', onClose);
            ws.off('error', onClose);
        });

        if (sendMsg) {
            sendRawMessage(sendMsg).catch(onClose);
        }

        return result;
    };

    /**
     * Wait for a message with a certain tag to be received
     * @param msgId the message tag to await
     * @param timeoutMs timeout after which the promise will reject
     */
    const waitForMessage = async (msgId, timeoutMs = defaultQueryTimeoutMs, maxRetries = retryConfig.maxRetries) => {
        let onRecv;
        let onErr;
        
        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                const result = await (0, Utils_1.promiseTimeout)(timeoutMs, (resolve, reject) => {
                    onRecv = resolve;
                    onErr = err => {
                        reject(err || new boom_1.Boom('Connection Closed', { 
                            statusCode: Types_1.DisconnectReason.connectionClosed 
                        }));
                    };
                    ws.on(`TAG:${msgId}`, onRecv);
                    ws.on('close', onErr);
                    ws.off('error', onErr);
                });
                
                return result;
            } catch (error) {
                if (attempt === maxRetries) {
                    throw error;
                }
                
                logger.warn({ attempt, msgId, error: error.message }, `waitForMessage retry ${attempt}`);
                await (0, Utils_1.delay)(getRetryDelay(attempt));
                
                // Check connection before retry
                if (!ws.isOpen) {
                    throw new boom_1.Boom('Connection closed during retry', {
                        statusCode: Types_1.DisconnectReason.connectionClosed
                    });
                }
            } finally {
                ws.off(`TAG:${msgId}`, onRecv);
                ws.off('close', onErr);
                ws.off('error', onErr);
            }
        }
    };

    /** send a query, and wait for its response. auto-generates message ID if not provided */
    const query = async (node, timeoutMs, maxRetries = retryConfig.maxRetries) => {
        if (!node.attrs.id) {
            node.attrs.id = generateMessageTag();
        }
        
        const msgId = node.attrs.id;
        let lastError;
        
        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                const [result] = await Promise.all([
                    waitForMessage(msgId, timeoutMs, 1), // No retry here, we handle at query level
                    sendNode(node)
                ]);
                
                if ('tag' in result) {
                    (0, WABinary_1.assertNodeErrorFree)(result);
                }
                
                return result;
            } catch (error) {
                lastError = error;
                logger.warn({ attempt, msgId, error: error.message }, `Query attempt ${attempt} failed`);
                
                if (attempt === maxRetries) break;
                
                // Check if we should retry based on error type
                const shouldRetry = error.message.includes('timeout') || 
                                  error.message.includes('ECONNRESET') ||
                                  error.message.includes('socket') ||
                                  error.statusCode === 500;
                
                if (!shouldRetry) break;
                
                // Exponential backoff
                const delay = getRetryDelay(attempt);
                await (0, Utils_1.delay)(delay);
                
                // Cek koneksi sebelum retry
                if (!ws.isOpen) {
                    throw new boom_1.Boom('Connection closed during retry', { 
                        statusCode: Types_1.DisconnectReason.connectionClosed 
                    });
                }
            }
        }
        
        throw lastError || new boom_1.Boom('Query failed after retries', {
            statusCode: Types_1.DisconnectReason.timedOut
        });
    };

    /** connection handshake */
    const validateConnection = async () => {
        let helloMsg = {
            clientHello: { ephemeral: ephemeralKeyPair.public }
        };
        helloMsg = WAProto_1.proto.HandshakeMessage.fromObject(helloMsg);
        
        logger.info({ browser, helloMsg }, 'connected to WA');
        const init = WAProto_1.proto.HandshakeMessage.encode(helloMsg).finish();
        
        const result = await awaitNextMessage(init);
        const handshake = WAProto_1.proto.HandshakeMessage.decode(result);
        logger.trace({ handshake }, 'handshake recv from WA');
        
        const keyEnc = await noise.processHandshake(handshake, creds.noiseKey);
        
        let node;
        if (!creds.me) {
            node = (0, Utils_1.generateRegistrationNode)(creds, config);
            logger.info({ node }, 'not logged in, attempting registration...');
        } else {
            node = (0, Utils_1.generateLoginNode)(creds.me.id, config);
            logger.info({ node }, 'logging in...');
        }
        
        const payloadEnc = noise.encrypt(WAProto_1.proto.ClientPayload.encode(node).finish());
        await sendRawMessage(WAProto_1.proto.HandshakeMessage.encode({
            clientFinish: {
                static: keyEnc,
                payload: payloadEnc,
            },
        }).finish());
        
        noise.finishInit();
        startKeepAliveRequest();
    };

    const getAvailablePreKeysOnServer = async () => {
        const result = await query({
            tag: 'iq',
            attrs: {
                id: generateMessageTag(),
                xmlns: 'encrypt',
                type: 'get',
                to: WABinary_1.S_WHATSAPP_NET
            },
            content: [
                { tag: 'count', attrs: {} }
            ]
        });
        
        const countChild = (0, WABinary_1.getBinaryNodeChild)(result, 'count');
        return +countChild.attrs.value;
    };

    /** generates and uploads a set of pre-keys to the server */
    const uploadPreKeys = async (count = Defaults_1.INITIAL_PREKEY_COUNT) => {
        try {
            await keys.transaction(async () => {
                logger.info({ count }, 'uploading pre-keys');
                const { update, node } = await (0, Utils_1.getNextPreKeysNode)({ creds, keys }, count);
                await query(node);
                ev.emit('creds.update', update);
                logger.info({ count }, 'uploaded pre-keys');
            });
        } catch (error) {
            logger.error({ error: error.message }, 'Failed to upload pre-keys');
            throw error;
        }
    };

    const uploadPreKeysToServerIfRequired = async (force = false) => {
        try {
            const preKeyCount = await getAvailablePreKeysOnServer();
            logger.info(`${preKeyCount} pre-keys found on server`);
            
            if (preKeyCount <= Defaults_1.MIN_PREKEY_COUNT || force) {
                await uploadPreKeys();
            }
        } catch (error) {
            logger.error({ error: error.message }, 'Failed to check/upload pre-keys');
            // Don't throw, this is a maintenance operation
        }
    };

    const onMessageReceived = (data) => {
        try {
            noise.decodeFrame(data, frame => {
                var _a;
                // reset ping timeout
                lastDateRecv = new Date();
                let anyTriggered = false;
                anyTriggered = ws.emit('frame', frame);
                
                // if it's a binary node
                if (!(frame instanceof Uint8Array)) {
                    const msgId = frame.attrs.id;
                    if (logger.level === 'trace') {
                        logger.trace({ xml: (0, WABinary_1.binaryNodeToString)(frame), msg: 'recv xml' });
                    }
                    
                    /* Check if this is a response to a message we sent */
                    anyTriggered = ws.emit(`${Defaults_1.DEF_TAG_PREFIX}${msgId}`, frame) || anyTriggered;
                    
                    /* Check if this is a response to a message we are expecting */
                    const l0 = frame.tag;
                    const l1 = frame.attrs || {};
                    const l2 = Array.isArray(frame.content) ? (_a = frame.content[0]) === null || _a === void 0 ? void 0 : _a.tag : '';
                    
                    for (const key of Object.keys(l1)) {
                        anyTriggered = ws.emit(`${Defaults_1.DEF_CALLBACK_PREFIX}${l0},${key}:${l1[key]},${l2}`, frame) || anyTriggered;
                        anyTriggered = ws.emit(`${Defaults_1.DEF_CALLBACK_PREFIX}${l0},${key}:${l1[key]}`, frame) || anyTriggered;
                        anyTriggered = ws.emit(`${Defaults_1.DEF_CALLBACK_PREFIX}${l0},${key}`, frame) || anyTriggered;
                    }
                    
                    anyTriggered = ws.emit(`${Defaults_1.DEF_CALLBACK_PREFIX}${l0},,${l2}`, frame) || anyTriggered;
                    anyTriggered = ws.emit(`${Defaults_1.DEF_CALLBACK_PREFIX}${l0}`, frame) || anyTriggered;
                    
                    if (!anyTriggered && logger.level === 'debug') {
                        logger.debug({ unhandled: true, msgId, fromMe: false, frame }, 'communication recv');
                    }
                }
            });
        } catch (error) {
            logger.error({ error: error.message }, 'Error processing received message');
            onUnexpectedError(error, 'onMessageReceived');
        }
    };

    const end = (error) => {
        if (closed) {
            logger.trace({ trace: error === null || error === void 0 ? void 0 : error.stack }, 'connection already closed');
            return;
        }
        
        closed = true;
        isReconnecting = false;
        
        logger.info({ 
            trace: error === null || error === void 0 ? void 0 : error.stack,
            reason: error?.output?.statusCode 
        }, error ? 'connection errored' : 'connection closed');
        
        clearInterval(keepAliveReq);
        clearTimeout(qrTimer);
        
        ws.removeAllListeners('close');
        ws.removeAllListeners('error');
        ws.removeAllListeners('open');
        ws.removeAllListeners('message');
        
        if (!ws.isClosed && !ws.isClosing) {
            try {
                ws.close();
            } catch (_a) { }
        }
        
        ev.emit('connection.update', {
            connection: 'close',
            lastDisconnect: {
                error,
                date: new Date()
            }
        });
        
        ev.removeAllListeners('connection.update');
    };

    const waitForSocketOpen = async () => {
        if (ws.isOpen) {
            return;
        }
        
        if (ws.isClosed || ws.isClosing) {
            throw new boom_1.Boom('Connection Closed', { statusCode: Types_1.DisconnectReason.connectionClosed });
        }
        
        let onOpen;
        let onClose;
        
        await new Promise((resolve, reject) => {
            onOpen = () => resolve(undefined);
            onClose = mapWebSocketError(reject);
            ws.on('open', onOpen);
            ws.on('close', onClose);
            ws.on('error', onClose);
        })
        .finally(() => {
            ws.off('open', onOpen);
            ws.off('close', onClose);
            ws.off('error', onClose);
        });
    };

    const startKeepAliveRequest = () => {
        keepAliveReq = setInterval(async () => {
            if (!lastDateRecv) {
                lastDateRecv = new Date();
            }
            
            const diff = Date.now() - lastDateRecv.getTime();
            
            /*
                check if it's been a suspicious amount of time since the server responded with our last seen
                it could be that the network is down
            */
            if (diff > keepAliveIntervalMs + 15000) { // Increased tolerance
                logger.warn({ diff, threshold: keepAliveIntervalMs + 15000 }, 'Connection seems lost, attempting recovery');
                
                if (!isReconnecting && !closed) {
                    attemptGracefulRecovery();
                }
            } else if (ws.isOpen) {
                // if its all good, send a keep alive request
                try {
                    await query({
                        tag: 'iq',
                        attrs: {
                            id: generateMessageTag(),
                            to: WABinary_1.S_WHATSAPP_NET,
                            type: 'get',
                            xmlns: 'w:p',
                        },
                        content: [{ tag: 'ping', attrs: {} }]
                    }, defaultQueryTimeoutMs, 1); // No retry for keepalive
                } catch (err) {
                    logger.warn({ error: err.message }, 'keep alive failed');
                    // Don't trigger recovery for single keepalive failure
                }
            } else {
                logger.warn('keep alive called when WS not open');
            }
        }, keepAliveIntervalMs);
    };

    /** i have no idea why this exists. pls enlighten me */
    const sendPassiveIq = (tag) => (query({
        tag: 'iq',
        attrs: {
            to: WABinary_1.S_WHATSAPP_NET,
            xmlns: 'passive',
            type: 'set',
        },
        content: [
            { tag, attrs: {} }
        ]
    }));

    /** logout & invalidate connection */
    const logout = async (msg) => {
        var _a;
        const jid = (_a = authState.creds.me) === null || _a === void 0 ? void 0 : _a.id;
        if (jid) {
            try {
                await sendNode({
                    tag: 'iq',
                    attrs: {
                        to: WABinary_1.S_WHATSAPP_NET,
                        type: 'set',
                        id: generateMessageTag(),
                        xmlns: 'md'
                    },
                    content: [
                        {
                            tag: 'remove-companion-device',
                            attrs: {
                                jid,
                                reason: 'user_initiated'
                            }
                        }
                    ]
                });
            } catch (error) {
                logger.warn({ error: error.message }, 'Failed to send logout node');
            }
        }
        end(new boom_1.Boom(msg || 'Intentional Logout', { statusCode: Types_1.DisconnectReason.loggedOut }));
    };
    
    /** This method was created by snowi, and implemented by KyuuRzy */
    /** hey bro, if you delete this text */
    /** you are the most cursed human being who likes to claim other people's property 😹🙌🏻 */
    const requestPairingCode = async (phoneNumber, pairKey) => {
        if (pairKey) {
            authState.creds.pairingCode = pairKey.toUpperCase();
        } else {
            authState.creds.pairingCode = (0, Utils_1.bytesToCrockford)((0, crypto_1.randomBytes)(5));
        }

        authState.creds.me = {
            id: (0, WABinary_1.jidEncode)(phoneNumber, 's.whatsapp.net'),
            name: '~'
        };

        ev.emit('creds.update', authState.creds);
        
        await sendNode({
            tag: 'iq',
            attrs: {
                to: WABinary_1.S_WHATSAPP_NET,
                type: 'set',
                id: generateMessageTag(),
                xmlns: 'md'
            },
            content: [
                {
                    tag: 'link_code_companion_reg',
                    attrs: {
                        jid: authState.creds.me.id,
                        stage: 'companion_hello',
                        should_show_push_notification: 'true'
                    },
                    content: [
                        {
                            tag: 'link_code_pairing_wrapped_companion_ephemeral_pub',
                            attrs: {},
                            content: await generatePairingKey()
                        },
                        {
                            tag: 'companion_server_auth_key_pub',
                            attrs: {},
                            content: authState.creds.noiseKey.public
                        },
                        {
                            tag: 'companion_platform_id',
                            attrs: {},
                            content: (0, Utils_1.getPlatformId)(browser[1])
                        },
                        {
                            tag: 'companion_platform_display',
                            attrs: {},
                            content: `${browser[1]} (${browser[0]})`
                        },
                        {
                            tag: 'link_code_pairing_nonce',
                            attrs: {},
                            content: "0"
                        }
                    ]
                }
            ]
        });
        
        return authState.creds.pairingCode;
    };

    async function generatePairingKey() {
        const salt = (0, crypto_1.randomBytes)(32);
        const randomIv = (0, crypto_1.randomBytes)(16);
        const key = await (0, Utils_1.derivePairingCodeKey)(authState.creds.pairingCode, salt);
        const ciphered = (0, Utils_1.aesEncryptCTR)(authState.creds.pairingEphemeralKeyPair.public, key, randomIv);
        return Buffer.concat([salt, randomIv, ciphered]);
    }

    const sendWAMBuffer = (wamBuffer) => {
        return query({
            tag: 'iq',
            attrs: {
                to: WABinary_1.S_WHATSAPP_NET,
                id: generateMessageTag(),
                xmlns: 'w:stats'
            },
            content: [
                {
                    tag: 'add',
                    attrs: {},
                    content: wamBuffer
                }
            ]
        });
    };

    // Event Listeners
    ws.on('message', onMessageReceived);
    
    ws.on('open', async () => {
        try {
            connectionAttempts = 0; // Reset on successful open
            await validateConnection();
        } catch (err) {
            logger.error({ err }, 'error in validating connection');
            end(err);
        }
    });
    
    ws.on('error', mapWebSocketError((error) => {
        logger.error({ error: error.message }, 'WebSocket error');
        if (!closed && !isReconnecting) {
            attemptGracefulRecovery();
        } else {
            end(error);
        }
    }));
    
    ws.on('close', () => {
        if (!closed && !isReconnecting) {
            logger.info('WebSocket closed unexpectedly, attempting recovery');
            attemptGracefulRecovery();
        } else if (!closed) {
            end(new boom_1.Boom('Connection Terminated', { 
                statusCode: Types_1.DisconnectReason.connectionClosed 
            }));
        }
    });
    
    // the server terminated the connection
    ws.on('CB:xmlstreamend', () => {
        if (!closed && !isReconnecting) {
            logger.info('Stream ended by server, attempting recovery');
            attemptGracefulRecovery();
        } else {
            end(new boom_1.Boom('Connection Terminated by Server', { 
                statusCode: Types_1.DisconnectReason.connectionClosed 
            }));
        }
    });
    
    // QR gen
    ws.on('CB:iq,type:set,pair-device', async (stanza) => {
        const iq = {
            tag: 'iq',
            attrs: {
                to: WABinary_1.S_WHATSAPP_NET,
                type: 'result',
                id: stanza.attrs.id,
            }
        };
        
        await safeSendNode(iq);
        
        const pairDeviceNode = (0, WABinary_1.getBinaryNodeChild)(stanza, 'pair-device');
        const refNodes = (0, WABinary_1.getBinaryNodeChildren)(pairDeviceNode, 'ref');
        const noiseKeyB64 = Buffer.from(creds.noiseKey.public).toString('base64');
        const identityKeyB64 = Buffer.from(creds.signedIdentityKey.public).toString('base64');
        const advB64 = creds.advSecretKey;
        
        let qrMs = qrTimeout || 60000; // time to let a QR live
        const genPairQR = () => {
            if (!ws.isOpen) {
                return;
            }
            
            const refNode = refNodes.shift();
            if (!refNode) {
                end(new boom_1.Boom('QR refs attempts ended', { 
                    statusCode: Types_1.DisconnectReason.timedOut 
                }));
                return;
            }
            
            const ref = refNode.content.toString('utf-8');
            const qr = [ref, noiseKeyB64, identityKeyB64, advB64].join(',');
            ev.emit('connection.update', { qr });
            qrTimer = setTimeout(genPairQR, qrMs);
            qrMs = qrTimeout || 20000; // shorter subsequent qrs
        };
        
        genPairQR();
    });
    
    // device paired for the first time
    ws.on('CB:iq,,pair-success', async (stanza) => {
        logger.debug('pair success recv');
        try {
            const { reply, creds: updatedCreds } = (0, Utils_1.configureSuccessfulPairing)(stanza, creds);
            logger.info({ me: updatedCreds.me, platform: updatedCreds.platform }, 'pairing configured successfully, expect to restart the connection...');
            
            ev.emit('creds.update', updatedCreds);
            ev.emit('connection.update', { isNewLogin: true, qr: undefined });
            await safeSendNode(reply);
        } catch (error) {
            logger.info({ trace: error.stack }, 'error in pairing');
            end(error);
        }
    });
    
    // login complete
    ws.on('CB:success', async (node) => {
        try {
            await uploadPreKeysToServerIfRequired();
            await sendPassiveIq('active');
            logger.info('opened connection to WA');
            
            clearTimeout(qrTimer);
            ev.emit('creds.update', { me: { ...authState.creds.me, lid: node.attrs.lid } });
            ev.emit('connection.update', { connection: 'open' });
            
            // Reset connection attempts on successful login
            connectionAttempts = 0;
        } catch (err) {
            logger.error({ err }, 'error opening connection');
            end(err);
        }
    });
    
    ws.on('CB:stream:error', (node) => {
        logger.error({ node }, 'stream errored out');
        const { reason, statusCode } = (0, Utils_1.getErrorCodeFromStreamError)(node);
        
        // Attempt recovery for certain stream errors
        if (statusCode !== Types_1.DisconnectReason.loggedOut && 
            statusCode !== Types_1.DisconnectReason.multideviceMismatch) {
            attemptGracefulRecovery();
        } else {
            end(new boom_1.Boom(`Stream Errored (${reason})`, { 
                statusCode, 
                data: node 
            }));
        }
    });
    
    // stream fail, possible logout
    ws.on('CB:failure', (node) => {
        const reason = +(node.attrs.reason || 500);
        end(new boom_1.Boom('Connection Failure', { 
            statusCode: reason, 
            data: node.attrs 
        }));
    });
    
    ws.on('CB:ib,,downgrade_webclient', () => {
        end(new boom_1.Boom('Multi-device beta not joined', { 
            statusCode: Types_1.DisconnectReason.multideviceMismatch 
        }));
    });
    
    ws.on('CB:ib,,offline_preview', (node) => {
        logger.info('offline preview received', JSON.stringify(node));
        safeSendNode({
            tag: 'ib',
            attrs: {},
            content: [{ tag: 'offline_batch', attrs: { count: '100' } }]
        }).catch(() => {
            // Ignore errors in offline preview response
        });
    });
    
    ws.on('CB:ib,,edge_routing', (node) => {
        const edgeRoutingNode = (0, WABinary_1.getBinaryNodeChild)(node, 'edge_routing');
        const routingInfo = (0, WABinary_1.getBinaryNodeChild)(edgeRoutingNode, 'routing_info');
        if (routingInfo?.content) {
            authState.creds.routingInfo = Buffer.from(routingInfo.content);
            ev.emit('creds.update', authState.creds);
        }
    });
    
    let didStartBuffer = false;
    process.nextTick(() => {
        var _a;
        if (creds.me?.id) {
            ev.buffer();
            didStartBuffer = true;
        }
        ev.emit('connection.update', { 
            connection: 'connecting', 
            receivedPendingNotifications: false, 
            qr: undefined 
        });
    });
    
    // called when all offline notifs are handled
    ws.on('CB:ib,,offline', (node) => {
        const child = (0, WABinary_1.getBinaryNodeChild)(node, 'offline');
        const offlineNotifs = +((child?.attrs.count) || 0);
        logger.info(`handled ${offlineNotifs} offline messages/notifications`);
        
        if (didStartBuffer) {
            ev.flush();
            logger.trace('flushed events for initial buffer');
        }
        ev.emit('connection.update', { receivedPendingNotifications: true });
    });
    
    // update credentials when required
    ev.on('creds.update', update => {
        var _a, _b;
        const name = update.me?.name;
        // if name has just been received
        if (creds.me?.name !== name) {
            logger.debug({ name }, 'updated pushName');
            safeSendNode({
                tag: 'presence',
                attrs: { name: name }
            }).catch(err => {
                logger.warn({ trace: err.stack }, 'error in sending presence update on name change');
            });
        }
        Object.assign(creds, update);
    });
    
    if (printQRInTerminal) {
        (0, Utils_1.printQRIfNecessaryListener)(ev, logger);
    }

    return {
        type: 'md',
        ws,
        ev,
        authState: {
            creds,
            keys 
        },
        signalRepository,
        get user() {
            return authState.creds.me;
        },
        generateMessageTag,
        query,
        waitForMessage,
        waitForSocketOpen,
        sendRawMessage,
        sendNode,
        safeSendNode,
        logout,
        end,
        onUnexpectedError,
        uploadPreKeys,
        uploadPreKeysToServerIfRequired,
        requestPairingCode,
        /** Waits for the connection to WA to reach a state */
        waitForConnectionUpdate: (0, Utils_1.bindWaitForConnectionUpdate)(ev),
        sendWAMBuffer,
        attemptGracefulRecovery, // Expose for manual recovery
    };
};

exports.makeSocket = makeSocket;

/**
 * map the websocket error to the right type
 * so it can be retried by the caller
 * */
function mapWebSocketError(handler) {
    return (error) => {
        handler(new boom_1.Boom(`WebSocket Error (${error?.message})`, { 
            statusCode: (0, Utils_1.getCodeFromWSError)(error), 
            data: error 
        }));
    };
}