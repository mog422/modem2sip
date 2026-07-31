#
# OpenWrt package for modem2sip.
#
# Drop this directory into a feed (see openwrt/README.md) and build with:
#
#   ./scripts/feeds update mine && ./scripts/feeds install modem2sip
#   make package/modem2sip/compile V=s
#
include $(TOPDIR)/rules.mk

PKG_NAME:=modem2sip
PKG_VERSION:=0.1.0
PKG_RELEASE:=1

PKG_SOURCE_PROTO:=git
PKG_SOURCE_URL:=https://github.com/mog422/modem2sip.git
# Pin a tag or commit for releases; a branch keeps following its tip.
PKG_SOURCE_VERSION:=main
PKG_MIRROR_HASH:=skip

PKG_MAINTAINER:=mog422 <admin@mog422.net>
PKG_LICENSE:=MIT

PKG_BUILD_DEPENDS:=rust/host
PKG_BUILD_PARALLEL:=1

include $(INCLUDE_DIR)/package.mk
include $(TOPDIR)/feeds/packages/lang/rust/rust-package.mk

# SQLite comes from the system library instead of being compiled into the
# binary (the `bundled-sqlite` default feature).
CARGO_PKG_ARGS+= --no-default-features

# alsa-sys and libsqlite3-sys ask pkg-config where their libraries are, and
# the pkg-config crate refuses to answer while cross-compiling unless this is
# set.  PKG_CONFIG_PATH itself already points into the staging directory.
CARGO_PKG_VARS+= PKG_CONFIG_ALLOW_CROSS=1

define Package/modem2sip
  SECTION:=net
  CATEGORY:=Network
  SUBMENU:=Telephony
  TITLE:=Expose an LTE modem as a SIP endpoint
  URL:=https://github.com/mog422/modem2sip
  DEPENDS:=$(RUST_ARCH_DEPENDS) \
	+modemmanager \
	+alsa-lib \
	+libsqlite3 \
	+kmod-usb-audio
endef

define Package/modem2sip/description
  Voice, SMS and MMS for one ModemManager modem, bridged to SIP.

  The modem is driven entirely over ModemManager's D-Bus API; voice runs
  through the ALSA card the modem exposes, bridged to RTP with G.711.
  SMS and MMS are stored in SQLite and announced to SIP peers with MESSAGE
  requests, with MMS attachments served over a small local HTTP API.

  One process serves one modem, selected in its configuration file, so
  several modems are served by several instances - the init script starts
  one per /etc/modem2sip/*.toml.
endef

define Package/modem2sip/conffiles
/etc/modem2sip/config.toml
endef

define Package/modem2sip/install
	$(INSTALL_DIR) $(1)/usr/bin
	$(INSTALL_BIN) $(PKG_INSTALL_DIR)/bin/modem2sip $(1)/usr/bin/

	$(INSTALL_DIR) $(1)/etc/init.d
	$(INSTALL_BIN) ./files/modem2sip.init $(1)/etc/init.d/modem2sip

	$(INSTALL_DIR) $(1)/etc/modem2sip
	$(INSTALL_CONF) ./files/config.toml $(1)/etc/modem2sip/config.toml

	$(INSTALL_DIR) $(1)/usr/libexec/modem2sip
	$(INSTALL_BIN) $(PKG_BUILD_DIR)/contrib/mms-bearer $(1)/usr/libexec/modem2sip/mms-bearer
endef

define Package/modem2sip/postinst
#!/bin/sh
[ -n "$${IPKG_INSTROOT}" ] && exit 0
echo "modem2sip: edit /etc/modem2sip/config.toml (at least [modem] imei and"
echo "           [sip]), then: /etc/init.d/modem2sip enable && start"
echo "           'modem2sip --list-modems' prints the values to fill in."
exit 0
endef

$(eval $(call BuildPackage,modem2sip))
