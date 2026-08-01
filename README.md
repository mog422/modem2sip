# modem2sip

Expose an LTE modem as a SIP endpoint: voice calls both ways, SMS, and MMS.

* **Modem control is done entirely through ModemManager over D-Bus.** No AT
  commands are sent by this program.
* **Voice runs over the ALSA card the modem exposes** (Quectel USB audio and
  similar), bridged to RTP with G.711.
* **SMS and MMS are stored in SQLite**; SIP peers get notified with a
  `MESSAGE` request, and MMS attachments are served over a small local HTTP
  API.
* **One process owns one modem.** The modem is selected in the config file, so
  several modems on one host are served by several instances.
* **The modem may be absent, appear late, disappear and come back.** While it
  is unusable SIP answers `503 Service Unavailable` with `Retry-After`; when
  it returns, service resumes without a restart.

Reference implementations that informed the design:
[asterisk-chan-quectel](https://github.com/IchthysMaranatha/asterisk-chan-quectel),
[asterisk-chan-modemmanager](https://github.com/koreapyj/asterisk-chan-modemmanager).

## Requirements

* Linux with ModemManager ≥ 1.18 running on the system bus, built with voice
  support (`mmcli -m 0 --voice-status` should work).
* A modem whose voice path is a sound card (`snd-usb-audio` for Quectel UAC
  builds). Modems that only do voice over a serial PCM port are not supported.
* Rust 1.75+, `libasound2-dev`, `libdbus-1-dev`, `pkg-config`, a C toolchain
  (rusqlite is built bundled).

```
sudo apt install build-essential pkg-config libasound2-dev
cargo build --release
```

SQLite is compiled in by default. To link the system library instead — what
distributions and the OpenWrt package do — build with
`--no-default-features` and install `libsqlite3-dev`.

For OpenWrt there is a package with a procd service in
[openwrt/](openwrt/README.md).

## Quick start

```
modem2sip --list-modems     # shows imei/device/primary_port + the matched ALSA card
modem2sip --list-cards      # shows every ALSA card on the system

cp config.example.toml /etc/modem2sip/config.toml
$EDITOR /etc/modem2sip/config.toml       # at minimum: [modem] imei, [sip] targets
modem2sip --config /etc/modem2sip/config.toml
```

## How the pieces fit together

```
   SIP UA / PBX                modem2sip                    ModemManager
  ─────────────      ┌───────────────────────────┐        ─────────────
   INVITE  ────────▶ │ sip::core ── gateway ─────┼──────▶ Voice.CreateCall
   RTP    ◀────────▶ │ media (RTP)   │           │        Call.Accept/Hangup
                     │      │        │           │
                     │   audio (ALSA)│           │
                     │      ▼        ▼           │
                     │  plughw:N,0   db (SQLite) │
   MESSAGE ◀───────▶ │ sms / mms ────┘           │◀─────  Messaging.Added
                     └───────────────┬───────────┘
   HTTP API ◀──────────────────────  │  ──────────────▶ MMSC (HTTP over the
                                                          data bearer)
```

### Choosing the right sound card

Card numbers move around between boots, and two identical modems produce two
identical-looking cards. ModemManager reports the modem's sysfs path
(`Device`), and every ALSA card has a `device` symlink back into sysfs, so the
card whose device node lives under the modem's device path is *that* modem's
card. That is what `audio.auto` does; `audio.card_hint` only disambiguates
when several cards match, and `audio.device` overrides everything.

If the driver publishes the audio device on the call object itself
(`Call.AudioPort`), that value wins when `audio.use_mm_audio_port = true`.

### Quectel: the USB voice path has to be switched on

On the Quectel family the sound card appears and streams happily while the
modem sends **no call audio to it**: `AT+QDAI` is already `5` (USB audio) but
`AT+QPCMV` stays `0`. Every call is silent until `AT+QPCMV=1,2` is issued —
which is exactly what `asterisk-chan-quectel` does, and what ModemManager has
no D-Bus API for.

[src/vendor.rs](src/vendor.rs) is the one place in this crate that talks AT,
and it exists only for that switch. It asks ModemManager to carry the command
first — `Modem.Command`, which serialises with MM's own traffic — and opens
the AT port itself only when MM refuses, which it does unless it was started
with `--debug`:

```toml
[audio]
vendor_audio_setup = "auto"   # "auto" (Quectel only) | "always" | "never"
```

* runs on every modem-ready event, so a replug or a modem reset is covered
  (the setting is volatile);
* re-checks before each call and only writes when the path is actually off;
* falls back to a port *ModemManager itself* classified as AT, preferring the
  last one (MM tends to keep the first for itself);
* re-checks whether ModemManager will relay commands whenever a modem becomes
  ready, so switching MM in or out of debug mode is picked up without
  restarting the gateway;
* warns if `AT+QDAI` is not `5`, but never changes it — that one is persistent
  and needs a modem reset, so it stays an operator decision;
* a failure is logged, not fatal: SMS keeps working on a modem whose voice
  path could not be switched on.

Measured on an EP06-E, average `|sample|` over a live call: **8** (silence)
before, **3200–3400** (speech) after.

For anything else a vendor needs, `[modem] ready_command` runs an arbitrary
command on every modem-ready event with `M2S_AT_PORT`, `M2S_AT_PORTS`,
`M2S_AUDIO_PORTS`, `M2S_ALSA_DEVICE`, `M2S_DEVICE`, `M2S_IMEI`,
`M2S_PRIMARY_PORT` and `M2S_MODEM_PATH` in its environment.

### Calls

* **SIP → mobile**: `INVITE sip:<number>@gateway` places the call with
  `Voice.CreateCall` + `Call.Start`. Alerting is answered with `183 Session
  Progress` and SDP so the caller hears the network itself (see below), and
  `200 OK` follows the modem's `active` state. Only G.711 (PCMU/PCMA) at
  20 ms is offered; anything else gets `488`.
* **mobile → SIP**: an incoming `Call` object triggers an `INVITE` to
  `sip.call_target`, or to the most recently registered contact if no target
  is configured. The network call is only accepted once SIP has answered and
  the audio path is open, so nothing is clipped.
* Only one call at a time (one modem, one audio card): further `INVITE`s get
  `486 Busy Here`.
* DTMF: SIP `INFO` (`application/dtmf-relay`) and RFC 2833 both map to
  `Call.SendDtmf`; digits reported by the modem are sent to SIP as `INFO`.

### Early media

The mobile network starts sending audio while it is still alerting: its own
ringback tone, the operator's announcements ("the number you have dialled is
not in service"), and IVRs that answer with early media. A bare `180 Ringing`
throws all of that away and leaves the caller listening to a tone their own
phone generates.

But the network does not always have something to play — a call can ring for
twenty-four seconds while the audio path carries nothing but its noise floor,
and a caller told to listen to *that* hears silence where `180` would have
had their own phone ring.

So the gateway does both, in the order the network decides. When alerting
starts it opens the audio path and answers `180 Ringing`, and it watches what
the modem is actually sending. The moment real audio appears it sends
`183 Session Progress` with SDP and the caller hears the network instead; if
nothing ever appears, the call just rings. The `200 OK` carries the same SDP
and the media session keeps running, so there is no gap when the call
connects. `[sip] early_media = false` stays on `180` throughout.

Measured on an EP06-E: audio is present in `ringing-out` at an average
`|sample|` of 6000–7500, and the caller hears it from the 183 onwards.

Every call reports what the caller was actually sent while it rang, because
"the caller heard nothing" and "the network sent nothing" look the same from
the SIP side:

```
INFO early media: the caller now hears the network level=3743
INFO ringing ended without an answer level=2957 ms=24360 early_media=true
INFO ringing ended without an answer level=0 ms=24480 early_media=false
```

`level` is the average `|sample|`: a few thousand is a ringback tone or an
announcement, and the noise floor of a silent path is about 20. If a caller
reports hearing nothing on a call whose level was in the thousands, the audio
was sent and their client did not play it.

This is measured rather than asked because there is nothing to ask:
ModemManager exposes nothing about QMI's alerting type — which is exactly the
network-or-handset distinction at stake — libqmi 1.38 does not define the
enum, and the modem sends no such TLV in its `ALL_CALL_STATUS` indication.

### DTMF on a VoLTE call

Neither of the two ways a modem is normally asked to produce a digit works on
an IMS call here:

```
Call.SendDtmf (QMI)  -> QMI protocol error (3): 'Internal'
AT+VTS=1             -> +CME ERROR: network rejected request
```

`AT+CLCC` shows the call, and `AT+QNWINFO` says `FDD LTE` with IMS
registered — a VoLTE call has no CS domain, and this firmware maps both
requests onto the circuit-switched procedure the network then refuses.
`mmcli --send-dtmf` fails identically, so it is not something the gateway can
fix by calling a different API.

It is not a permanent property of the modem either: whether the firmware can
signal a digit depends on the codec negotiated for the individual call, and
`Call.SendDtmf` does succeed on some of them.

What the gateway does instead, in both directions:

* **SIP → mobile**: ask ModemManager first, on *every* call; when it refuses,
  generate the tones and play them into the modem's uplink audio, exactly
  like a handset with in-band signalling. Only that call keeps to the tone
  generator — the next one asks ModemManager again (`[rtp] dtmf_method`).
* **mobile → SIP**: watch the audio coming from the network for DTMF and
  relay what it finds as SIP `INFO` (`[rtp] detect_inband_dtmf`). While the
  gateway is playing a digit the detector is muted, so the modem's sidetone
  cannot echo a digit back to the SIP peer.

For comparison, `asterisk-chan-quectel`'s `DTMFforIMS` branch solves the
**receive** direction the same way in spirit but with the modem's own
detector: it enables `AT+QTONEDET` and turns the `+QTONEDET: <ascii>` URC
(or Simcom's `+RXDTMF:`) into an Asterisk DTMF frame. That is the better
detector — it runs before the vocoder — but it needs exclusive ownership of
the AT port, and here ModemManager holds both AT ports open and the modem's
URC port is `usbat`. Stealing URCs from ModemManager would be unreliable in
both directions, so the gateway detects the same tones in the audio instead.
That branch does not change the *send* direction; it still uses `AT+VTS`.

### Marking messages: `messagetype=sms`

Everything the gateway hands to SIP carries `messagetype=sms` as a parameter
of the To header:

```
MESSAGE sip:phone1@192.168.1.10:5060 SIP/2.0
To: <sip:phone1@192.168.1.10:5060>;messagetype=sms
Content-Type: text/plain
```

The same marker is **required** on anything the gateway is asked to send. A
`MESSAGE` whose To header does not say `messagetype=sms` is rejected with
`415 Unsupported Media Type` and nothing goes on the air, so a message meant
for some other purpose can never be turned into an SMS by accident. The
parameter is matched case-insensitively, and a sender that puts it inside the
URI instead is still understood.

This applies to MMS submissions over SIP too. The HTTP API is a separate
interface and is unaffected.

### SMS

Incoming messages are stored in SQLite and forwarded as a SIP `MESSAGE`
(`text/plain`) whose `From` user part is the sender's number. They are deleted
from the modem/SIM only after the database write succeeds.

Sending: a SIP `MESSAGE` to `sip:<number>@gateway` with a `text/plain` body
and `To: <sip:<number>@gateway>;messagetype=sms`, or
`POST /sms {"to": "...", "text": "..."}`. Segmentation and GSM7/UCS2
encoding are ModemManager's job.

### MMS

ModemManager has no MMS API, so MMS is handled here:

1. The carrier sends a binary SMS containing a WAP push. It is decoded
   (`mms::wsp`, `mms::pdu`) into an `M-Notification.ind`.
2. The notification is stored immediately, then the body is fetched from
   `X-Mms-Content-Location` over HTTP (`mms.proxy` / `mms.interface` /
   `mms.local_ip` control which path that traffic takes), decoded as
   `M-Retrieve.conf`, and every part is written to
   `storage.dir/attachments/<id>/` with a row in `attachments`.
   `M-Acknowledge.ind` is posted back to the MMSC.
3. SIP only gets a **simplified** version: a `text/plain` `MESSAGE` with the
   sender, subject, body text and one line per attachment including a download
   URL pointing at the HTTP API.

Sending an MMS (attachments make SIP `MESSAGE` a poor carrier, so the HTTP API
is the primary route):

```
curl -X POST http://127.0.0.1:8088/mms -H 'Content-Type: application/json' -d '{
  "to": ["+821012345678"],
  "subject": "photo",
  "text": "here you go",
  "attachments": [{"content_type": "image/jpeg", "name": "p.jpg", "path": "/tmp/p.jpg"}]
}'
```

The same JSON body may be sent as a SIP `MESSAGE` with
`Content-Type: application/json`, using `data_base64` instead of `path` for
inline attachments.

**Carrier settings are required**: `mms.mmsc` (and `mms.proxy` on carriers
that use a WAP gateway) are operator specific.

MMS traffic must leave through the modem, so the gateway binds those sockets
to the data bearer's interface and source address, which it reads from
ModemManager at request time — no configuration, and it follows the address
across re-attaches. Pin `mms.interface` / `mms.local_ip` only to override it.

The bearer itself still has to exist and be routable, and that is a host
networking job: [contrib/mms-bearer](contrib/mms-bearer) connects it and adds
a default route **in a private routing table** plus rules matching the
modem's source address and interface, so the box keeps its own default route
(and your SSH session):

```
install -Dm755 contrib/mms-bearer /usr/local/libexec/modem2sip/mms-bearer
install -Dm644 systemd/modem2sip-mms-bearer@.service /etc/systemd/system/
systemctl enable --now modem2sip-mms-bearer@lte.ktfwing.com.service   # instance = APN
```

`POST /messages/{id}/retrieve` re-runs a download that never happened —
useful for notifications that arrived while MMS was disabled or while the
bearer was down.

#### What real carrier traffic taught us

Tested against KT (`http://mmsc.ktfwing.com:9082`):

* **Encode the charset as a short integer.** WSP allows a value ≤ 127 as
  either a short or a long integer; KT's MMSC accepts the submission either
  way and then bounces it back by SMS as
  *"미지원 컨텐츠가 포함되어 있습니다"* (unsupported content) if UTF-8 is
  written as the long form `01 6A` instead of the canonical `EA`. Every real
  PDU from the network uses the short form.
* **MMSC names must be resolved by the carrier, not by the host.** The
  gateway sends its own DNS queries to the resolvers the bearer reports,
  from the bearer's address ([src/mms/dns.rs](src/mms/dns.rs)), and only
  falls back to the system resolver if that fails. MMSC names are often
  absent from public DNS, and when they are present the answer can be
  useless: `mmsc.ktfwing.com` publishes an IPv6 ULA that nothing outside KT
  can reach, while KT's own resolvers return private IPv4 addresses — and
  retrieval URLs point at yet other hosts (`s-mmsc`, `d-mmsc`) that are
  carrier-internal too. Every resolved address is then tried in turn, IPv4
  first. Override with `mms.dns` if a carrier needs something else.

### HTTP API

| Method | Path                                   | Purpose                              |
|--------|----------------------------------------|--------------------------------------|
| GET    | `/health`                              | modem state, ALSA card, signal (503 when down) |
| GET    | `/cards`                               | ALSA cards visible to the process    |
| GET    | `/messages?limit=&before=`             | stored SMS/MMS, newest first         |
| GET    | `/messages/{id}`                       | one message with its attachments     |
| GET    | `/messages/{id}/attachments/{index}`   | attachment download                  |
| POST   | `/sms`                                 | `{"to","text"}`                      |
| POST   | `/mms`                                 | see above                            |

Set `http.token` to require `Authorization: Bearer <token>`. The API binds to
localhost by default; it has no TLS, so put it behind a proxy if you expose
it. Attachments are served with `nosniff` and `Content-Disposition:
attachment`, because both the bytes and the declared media type come from
whoever sent the MMS.

## Exposure

Nothing here is safe to put on an untrusted network as it stands: SIP is
plaintext UDP and the HTTP API has no TLS. What the gateway does enforce:

* `sip.auth` challenges inbound `INVITE`/`MESSAGE`/`REGISTER` with digest.
  Each nonce is accepted once (or once per `nc` value with `qop=auth`), and
  the realm and Request-URI are checked, so a captured `Authorization` header
  cannot be replayed onto a different request.
* In-dialog `BYE` and `INFO` are matched on the Call-ID **and** both dialog
  tags, so knowing the Call-ID is not enough to hang up a call or inject
  digits into it.
* RTP is only accepted from the address signalled in SDP. Symmetric RTP still
  latches onto whatever port the peer sends from, which is what makes it work
  behind NAT, but not onto a different host.
* `sip.allow` restricts which source addresses are answered at all.

With `sip.auth` unset and `sip.allow` empty — the defaults — anyone who can
reach the SIP port can place calls and send messages, and one unauthenticated
`REGISTER` is enough to become the target inbound calls and SMS are delivered
to. Set at least one of the two on any interface you do not fully control.

## Behaviour when the modem is missing

The supervisor (`mm::watcher`) reconnects to the system bus, re-scans on every
ObjectManager change plus every 5 s, enables the modem if it comes up
disabled, waits for it to leave `initializing`/`searching`, and re-runs the
whole sequence whenever the device returns. Meanwhile:

* `INVITE`, `MESSAGE` → `503` + `Retry-After`
* `OPTIONS` → `503` (`sip.options_reflect_modem`), so an upstream proxy can
  use it as a health probe
* a call in progress when the modem vanishes is torn down with `BYE`
* `REGISTER` keeps working, so UAs stay bound and get calls as soon as the
  modem is back

### Where the media goes

`rtp.bind` picks the address the media sockets live on. Left unset it follows
SIP, which is what a single-homed host wants; set it when the audio has to
leave by a different route than the signalling, or to `0.0.0.0` to listen
everywhere.

A specific `rtp.bind` is also what the SDP advertises, because that is the
only address the peer can reach the media on. `rtp.public_ip` overrides just
the advertised half, for the case where the socket is bound to a private
address and the peer sees a different one. Both are checked at start-up, so a
typo stops the process instead of silently sending the audio somewhere else.

`sip.public_ip` still governs Via and Contact.

## Running several modems

One process per modem, each with its own config: different `[modem]` matcher,
`sip.bind` port, `rtp.port_min/max` range, `storage.dir`, and `http.bind`.
The ALSA card is resolved per modem from sysfs, so the instances never fight
over each other's audio.

## Systemd

```ini
[Unit]
Description=modem2sip (%i)
After=ModemManager.service network-online.target
Wants=ModemManager.service

[Service]
ExecStart=/usr/local/bin/modem2sip --config /etc/modem2sip/%i.toml
Restart=always
RestartSec=5
# needed only if mms.interface is used (SO_BINDTODEVICE)
AmbientCapabilities=CAP_NET_RAW
SupplementaryGroups=audio dialout

[Install]
WantedBy=multi-user.target
```

## Verified on real hardware

Arch Linux, ModemManager 1.24.2, Quectel **EP06-E** (QMI, `cdc-wdm0`) with its
USB sound card (`plughw:EP06E,0`, S16_LE mono 8 kHz), live KT SIM.

| What | Result |
|---|---|
| Modem selection by IMEI, ALSA card via sysfs | card 1 matched from the modem's `usb9/9-1` path |
| Auto-enable of a `disabled` modem | `disabled` → `enabled` → ready in ~5 s |
| Outbound call (SIP → mobile) | `INVITE` → 100 → 180 (`ringing-out`) → 200 (`active`) |
| Voice media | 591 RTP packets / 20 ms, PCMA, avg `\|sample\|` 3400 (speech) |
| `BYE` teardown | 200 OK, modem call hung up, ALSA closed |
| SMS receive | 4 messages adopted from the SIM, Korean text intact, stored + `MESSAGE` to SIP |
| SMS send + round trip | SIP `MESSAGE` → 202 → delivered → came back as an inbound SMS in ~1 s |
| Duplicate suppression | re-adoption after restart/replug stored nothing twice |
| MMS end to end (live KT MMSC) | sent to our own number, notification came back, auto-retrieved, JPEG byte-identical, SIP client got the summary |
| MMS receive (real carrier messages) | 4 KT notifications decoded and downloaded, Korean subjects intact |
| Modem hot-unplug (USB deauthorize) | detected instantly, `OPTIONS`/`INVITE`/`/health` → 503 |
| Modem return (USB reset) | reappeared as `Modem/1`, IMEI match followed it, ready again |
| ModemManager restart | session re-established, modem re-adopted |
| Registrar, ACL, digest, dialog checks | `REGISTER` 200, out-of-dialog `INFO` → 481 |
| Built-in Quectel voice-path setup | `AT+QPCMV=1,2` sent on ready, skipped when already on, audio avg 3241 |
| Digest-authenticated softphone on another subnet | `REGISTER` 200, `INVITE` → audio (493 packets, avg 3210), inbound SMS delivered as `MESSAGE` |
| `cargo test` | 56 passed |

Not verifiable from this host: an **inbound** call (needs an external caller)
and MMS **receive** (needs a real WAP push plus carrier MMSC settings); both
paths are exercised only by unit tests and code review.

The hardware rows above were measured before the transaction-layer, teardown
and RTP-acceptance rework; only the test suite has been re-run since. Worth
repeating on a modem before trusting them again.

## Limitations

* **In-band DTMF is only as good as the vocoder.** When the modem refuses to
  signal digits (see above) the gateway plays the tones through the voice
  path, so AMR/AMR-WB compression sits between them and the far end's
  detector. It is what analogue gateways have always done, but it is not as
  reliable as signalled DTMF.
* SIP over UDP only; no TLS/SRTP and no video. A re-INVITE is answered and
  the media endpoint follows it, so hold/resume should survive, but neither
  that nor transfer has been tried against a real PBX.
* G.711 only. Wide-band modem cards are resampled to 8 kHz.
* One concurrent call per modem.
* MMS over HTTPS MMSC URLs is not supported (plain HTTP only, which is what
  MMSCs use in practice).
* USB *deauthorize* is not a clean unplug: ModemManager 1.24.2 can get stuck
  mid-setup afterwards and never re-publishes the modem (it stays in
  `running setup for device`). A real replug or a USBDEVFS reset recovers it.
  modem2sip keeps waiting and answering 503 throughout.
