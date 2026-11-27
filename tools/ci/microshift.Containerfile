# Pins for the VM lanes, never built: the builder of the VM disk and the
# MicroShift image the disk is made from. Dependabot bumps them, and the
# bump PR runs the VM lanes on the new disk.
FROM quay.io/centos-bootc/bootc-image-builder:latest@sha256:a829b782ecee4764edcac472d5672a76f20002143618fcfbd17dc1cc7319e642 AS bib
FROM ghcr.io/epheo/microshift:latest@sha256:857eb17e06881f4a529e837379989750c5ff5e37c2aef8445dfe374e8d2e0a10
