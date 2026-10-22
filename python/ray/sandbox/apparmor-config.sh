#!/bin/bash
if test -f /proc/sys/kernel/unprivileged_userns_apparmor_policy; then
	sudo sysctl -w kernel.unprivileged_userns_apparmor_policy=1
fi
if test -f /proc/sys/kernel/apparmor_restrict_unprivileged_userns; then
	# Ubuntu-specific setting
	sudo sysctl -w kernel.apparmor_restrict_unprivileged_userns=0
fi
