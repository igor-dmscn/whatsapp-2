// The browser's half of call signalling.
//
// Kept out of both the socket and React, for the same reason the transcript is: it is the
// piece most likely to be wrong, and the only way to be sure of it is to be able to drive
// it without either.
//
// Two planes, and only one of them is here. Signalling — offers, answers, who is present —
// rides the WebSocket the client already holds. Media goes straight to the forwarding
// server over UDP and never touches this file.

/** Frames the server sends about calls. */
export type CallFrame =
  | { type: 'call.joined'; call_id: string; conversation_id: string; state: string; participants: Participant[]; sdp: string }
  | { type: 'call.current'; call_id: string; conversation_id: string; state: string; participants: Participant[] }
  | { type: 'call.none'; conversation_id: string }
  | { type: 'call.offer'; call_id: string; sdp: string }
  | { type: 'call.left'; call_id: string }

export type Participant = {
  account_id: string
  device_id: string
}

/** CallState is what the interface renders.
 *
 *  `ringing` is a call somebody started and nobody else has joined — which is what makes a
 *  phone ring, and is a different thing to show from a call in progress. */
export type CallState = {
  callID: string
  conversationID: string
  state: 'ringing' | 'active' | string
  participants: Participant[]
  /** joined is whether *this* client is in it. A call can be live and not yours, which is
   *  the state an incoming ring is. */
  joined: boolean
  /** muted is local: the track is disabled rather than removed, so muting does not
   *  renegotiate and unmuting does not need a new keyframe from anyone. */
  muted: boolean
  /** remote is one media stream per other participant, keyed by the track's stream id. */
  remote: Map<string, MediaStream>
  local: MediaStream | null
}

export type CallOptions = {
  /** send puts a frame on the client's socket. */
  send: (frame: unknown) => void
  /** onChange fires whenever anything the interface renders has changed. */
  onChange: (state: CallState | null) => void
  onError: (failure: unknown) => void
  /** media captures the camera and microphone. Injected so tests can supply tracks
   *  without a device — getUserMedia in a headless browser needs a fake device flag, and a
   *  test that depends on a browser flag is a test that breaks on a different browser. */
  media?: () => Promise<MediaStream>
}

/** iceServers is empty on purpose.
 *
 *  Everything here is on one network, and a public STUN lookup would add a round trip to
 *  every join for a reflexive candidate nothing uses. A deployment across the internet needs
 *  STUN and probably TURN, which is configuration rather than code. */
const iceServers: RTCIceServer[] = []

/**
 * Call is one client's participation in one call.
 *
 * Created per call rather than per session, because a WebRTC peer connection cannot be
 * reused: leaving a call closes its transport, and joining the next one negotiates a new
 * one from nothing.
 */
export class Call {
  private connection: RTCPeerConnection | null = null
  private state: CallState | null = null
  /** negotiating serialises *every* exchange that touches this connection.
   *
   *  WebRTC permits one negotiation at a time, and the rule has to cover the answer to our
   *  own join as well as the offers the server sends — not just the offers. The first
   *  version of this chained only the offers, so a client that received `call.joined` and
   *  `call.offer` in the same instant ran two exchanges at once and lost one of them. That
   *  happens exactly when the other participant's tracks reach the server around the moment
   *  this client joins, which is a race: it presents as media arriving in one direction and
   *  not the other, with the failing direction varying between runs. */
  private negotiating: Promise<void> = Promise.resolve()

  constructor(private readonly options: CallOptions) {}

  /** current is what the interface should render, or null when there is no call. */
  current(): CallState | null {
    return this.state
  }

  /**
   * join captures the camera, offers, and waits for the server's answer.
   *
   * The offer is complete before it is sent — ICE gathering finishes first — so signalling
   * is a single exchange in each direction. The alternative, trickling candidates as they
   * are found, is faster to first media and an entire asynchronous protocol on both sides;
   * the server established in phase 8 that it does not need one.
   */
  async join(conversationID: string): Promise<void> {
    if (this.connection) return

    const capture = this.options.media ?? (() => navigator.mediaDevices.getUserMedia({ audio: true, video: true }))
    const local = await capture()

    const connection = new RTCPeerConnection({ iceServers })
    this.connection = connection

    for (const track of local.getTracks()) connection.addTrack(track, local)

    connection.ontrack = (event) => this.receive(event)
    connection.onconnectionstatechange = () => {
      // A transport that failed is a call that has ended for this client, whatever the
      // signalling said. Reported so the interface stops showing tiles for media that is
      // not arriving.
      if (connection.connectionState === 'failed') {
        this.options.onError(new Error('the call connection failed'))
        void this.leave()
      }
    }

    this.state = {
      callID: '',
      conversationID,
      state: 'ringing',
      participants: [],
      joined: true,
      muted: false,
      remote: new Map(),
      local,
    }
    this.publish()

    const offer = await connection.createOffer()
    await connection.setLocalDescription(offer)
    await gathered(connection)

    this.options.send({
      type: 'call.join',
      conversation_id: conversationID,
      sdp: connection.localDescription?.sdp ?? '',
    })
  }

  /** apply handles a frame the server sent about a call. */
  apply(frame: CallFrame): void {
    switch (frame.type) {
      case 'call.joined':
        this.queue(() => this.joined(frame))
        break

      case 'call.current':
        // A live call this client is not in, which is what an incoming ring looks like
        // when the notification was missed and the client asked instead.
        if (!this.state?.joined) {
          this.state = {
            callID: frame.call_id,
            conversationID: frame.conversation_id,
            state: frame.state,
            participants: frame.participants,
            joined: false,
            muted: false,
            remote: new Map(),
            local: null,
          }
          this.publish()
        } else if (this.state.callID === frame.call_id) {
          // Somebody joined or left one we are in.
          this.state = { ...this.state, state: frame.state, participants: frame.participants }
          this.publish()
        }
        break

      case 'call.none':
        if (this.state && !this.state.joined && this.state.conversationID === frame.conversation_id) {
          this.state = null
          this.publish()
        }
        break

      case 'call.offer':
        // The server has something new to send us — somebody else joined.
        this.queue(() => this.answer(frame))
        break

      case 'call.left':
        this.release()
        break
    }
  }

  /** queue runs work after everything already queued, and reports what it threw.
   *
   *  The chain is never broken by a failure: one exchange that could not be completed must
   *  not stop the next, or a single lost offer ends the call for that participant. */
  private queue(work: () => Promise<void>): void {
    this.negotiating = this.negotiating
      .then(work)
      .catch((failure) => this.options.onError(failure))
  }

  private async joined(frame: Extract<CallFrame, { type: 'call.joined' }>): Promise<void> {
    if (!this.connection || !this.state) return

    await this.connection.setRemoteDescription({ type: 'answer', sdp: frame.sdp })
    this.state = {
      ...this.state,
      callID: frame.call_id,
      state: frame.state,
      participants: frame.participants,
    }
    this.publish()
  }

  /** answer replies to an offer the server sent. */
  private async answer(frame: Extract<CallFrame, { type: 'call.offer' }>): Promise<void> {
    const connection = this.connection
    if (!connection) return

    await connection.setRemoteDescription({ type: 'offer', sdp: frame.sdp })
    const answer = await connection.createAnswer()
    await connection.setLocalDescription(answer)
    await gathered(connection)

    this.options.send({
      type: 'call.answer',
      call_id: frame.call_id,
      sdp: connection.localDescription?.sdp ?? '',
    })
  }

  /** receive records an arriving track against the participant it belongs to. */
  private receive(event: RTCTrackEvent): void {
    if (!this.state) return

    // Keyed by stream rather than by track: audio and video from one person arrive as two
    // tracks in one stream, and a tile shows both. Keying by track would give everybody two
    // tiles, one of them silent and one of them blank.
    //
    // The fallback is not defensive padding. A track whose media section carries no msid
    // arrives with an empty streams array, and the early return that used to be here
    // dropped it silently — a negotiated connection delivering packets to nothing, which
    // looks exactly like a server that never forwarded.
    const stream = event.streams[0] ?? new MediaStream([event.track])
    if (stream.getTracks().length === 0) return

    const remote = new Map(this.state.remote)
    remote.set(stream.id, stream)
    this.state = { ...this.state, remote }
    this.publish()
  }

  /** mute disables the local tracks without removing them.
   *
   *  Disabled rather than removed, and the difference matters: removing a track
   *  renegotiates, and coming back needs a fresh keyframe from everyone. Disabling sends
   *  silence, which is what mute means. */
  mute(muted: boolean): void {
    if (!this.state?.local) return

    for (const track of this.state.local.getAudioTracks()) track.enabled = !muted
    this.state = { ...this.state, muted }
    this.publish()
  }

  /** leave tells the server and releases the camera. */
  async leave(): Promise<void> {
    const callID = this.state?.callID
    if (callID) this.options.send({ type: 'call.leave', call_id: callID })
    this.release()
  }

  /** ask requests the current call in a conversation, if any. */
  ask(conversationID: string): void {
    this.options.send({ type: 'call.active', conversation_id: conversationID })
  }

  /** release closes the transport and stops the camera.
   *
   *  Stopping the tracks is what turns the camera light off. A client that closed its
   *  connection and left the tracks running is a privacy bug, and it is the one users
   *  notice immediately. */
  private release(): void {
    for (const track of this.state?.local?.getTracks() ?? []) track.stop()
    this.connection?.close()
    this.connection = null
    this.state = null
    this.publish()
  }

  private publish(): void {
    this.options.onChange(this.state)
  }
}

/** gathered resolves when ICE gathering has finished.
 *
 *  Resolved immediately when it already has: a renegotiation on a connected transport
 *  gathers nothing new, and waiting for an event that will not fire again would hang the
 *  exchange. */
function gathered(connection: RTCPeerConnection): Promise<void> {
  if (connection.iceGatheringState === 'complete') return Promise.resolve()

  return new Promise((resolve) => {
    const check = () => {
      if (connection.iceGatheringState === 'complete') {
        connection.removeEventListener('icegatheringstatechange', check)
        resolve()
      }
    }
    connection.addEventListener('icegatheringstatechange', check)
    // A timeout as well, because a candidate that never arrives must not stop a call
    // starting: what has been gathered by now is usually enough on a local network.
    setTimeout(() => {
      connection.removeEventListener('icegatheringstatechange', check)
      resolve()
    }, gatheringTimeout)
  })
}

/** gatheringTimeout bounds how long a client waits for candidates. Two seconds, against
 *  NF-3's two seconds for join to first media — so a slow gather costs the budget rather
 *  than the call. */
const gatheringTimeout = 2000
