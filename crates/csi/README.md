# csi

Generated CSI v1 bindings (tonic + prost) from proto/csi.proto, copied from container-storage-interface/spec v1.11.0 (Apache-2.0, proto/LICENSE).
protox compiles the proto at build time, so no protoc is needed.
Hand-written code: only `bind_unix`, the socket bind both roles share (a leftover socket is replaced, any other file is refused). Services are implemented in controller and agent.
