module github.com/Nomadcxx/sysc-notify

// The module calls dbus.Connect and net.DialTimeout, so it needs a standard
// library without GO-2026-4971 (net, fixed in go1.26.3), GO-2026-4602 (os,
// fixed in go1.26.1) or GO-2026-4601 (net/url, fixed in go1.26.1). CI reads
// this directive for the toolchain it installs, so the floor here is the
// floor everywhere.
go 1.26.10

require (
	github.com/godbus/dbus/v5 v5.2.4
	golang.org/x/sys v0.49.0
)
