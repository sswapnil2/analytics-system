# Runtime image for prebuilt release binaries; see .github/workflows/release.yml.
# Expects dist/amd64/pulse and dist/arm64/pulse (the unpacked release tarballs).
# ponytail: packages CI-built binaries instead of compiling in Docker (a release build takes
# ~15 min); add a cargo build stage if people need to build images from source.
FROM debian:bookworm-slim
ARG TARGETARCH
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 --home-dir /data pulse \
 && mkdir /data && chown pulse:pulse /data
COPY --chown=root:root dist/${TARGETARCH}/pulse /opt/pulse
ENV PULSE_HOST=0.0.0.0 \
    PORT=8080 \
    PULSE_DATA_DIR=/data \
    PULSE_WEB_DIST_DIR=/opt/pulse/web/dist
USER pulse
VOLUME /data
EXPOSE 8080
ENTRYPOINT ["/opt/pulse/pulse-node", "--no-quic"]
