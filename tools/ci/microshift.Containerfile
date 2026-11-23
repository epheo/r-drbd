# Pins for the VM lanes, never built: the builder of the VM disk and the
# MicroShift image the disk is made from. Dependabot bumps them, and the
# bump PR runs the VM lanes on the new disk.
FROM quay.io/centos-bootc/bootc-image-builder:latest@sha256:dab86ee6b46dcade411b37f4e70b07d93b6a06da0dbfec6fe89247a220e0d9f8 AS bib
FROM ghcr.io/epheo/microshift:latest@sha256:86a49aa882ec6b7b87b0b10431744e14819514cb2805e2c668b1fe0ba7a20100
