# Runtime image for CI-built release tarballs; see .github/workflows/release.yml.
# Expects dist/amd64/pulse and dist/arm64/pulse (the unpacked release tarballs).
# To build a Docker image locally from source, use Dockerfile.source instead:
#   docker build -f Dockerfile.source -t pulse:local .
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
