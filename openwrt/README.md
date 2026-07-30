# OpenWrt package

Builds `modem2sip` for OpenWrt and installs a procd service that runs one
instance per configuration file.

## Building

The package is not in the official feeds, so point a custom feed at this
repository. In your OpenWrt buildroot:

```
echo "src-git modem2sip https://github.com/mog422/modem2sip.git" >> feeds.conf.default
./scripts/feeds update modem2sip
./scripts/feeds install modem2sip
make menuconfig            # Network -> Telephony -> modem2sip
make package/modem2sip/compile V=s
```

The feed picks the package up from `openwrt/modem2sip/`, and the Makefile
fetches the sources over git — set `PKG_SOURCE_VERSION` to a tag or commit
for a reproducible build instead of following `main`.

To build from a checkout you are editing, drop `PKG_SOURCE_*` and add:

```make
USE_SOURCE_DIR:=/path/to/modem2sip
```

Rust builds the host toolchain first (`rust/host`), which takes a while on
the first run. The resulting binary is a few megabytes: check your flash
budget, and consider extroot if you plan to receive MMS.

## What gets installed

| Path | |
|---|---|
| `/usr/bin/modem2sip` | the gateway |
| `/etc/init.d/modem2sip` | procd service, one instance per `*.toml` |
| `/etc/modem2sip/config.toml` | configuration (kept across sysupgrade) |
| `/usr/libexec/modem2sip/mms-bearer` | optional data-bearer helper |

## Setting it up

```
modem2sip --list-modems          # values for [modem], and the ALSA card found
modem2sip --list-cards           # every sound card on the box
vi /etc/modem2sip/config.toml    # at least [sip.auth] and [http] token
/etc/init.d/modem2sip enable
/etc/init.d/modem2sip start
logread -f -e modem2sip
```

A second modem is a second file — copy `config.toml` to e.g.
`/etc/modem2sip/lte2.toml` and give it its own `[modem]` matcher, `sip.bind`
port, `rtp.port_min/max` range, `storage.dir` and `http.bind`. The init script
starts one process per file and names the procd instance after it.

Package dependencies pull in `modemmanager`, `alsa-lib`, `libsqlite3-0` and
`kmod-usb-audio` (the modem's sound card is the voice path). SQLite is the
system library rather than compiled into the binary, so the package builds
with `--no-default-features`.

## MMS

MMS traffic has to leave through the mobile data connection. On OpenWrt the
normal way is to let netifd manage it — a `wwan` interface with
`option proto modemmanager` — and modem2sip then binds its MMS sockets to
whatever bearer ModemManager reports, resolving MMSC names through the
carrier's own DNS servers. Nothing else needs configuring.

If you manage the bearer by hand instead, `mms-bearer` connects it and adds a
default route in a private routing table so the router keeps its own:

```
/usr/libexec/modem2sip/mms-bearer lte.ktfwing.com
```
