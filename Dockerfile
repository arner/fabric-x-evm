FROM scratch
ARG TARGETARCH
ARG VERSION
ARG CREATED
ARG REVISION
LABEL org.opencontainers.image.title="evm" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.created="${CREATED}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.source="https://github.com/Chaguar/evm"
COPY release/linux-${TARGETARCH}/fxevm /usr/local/bin/fxevm
USER 65534:65534
EXPOSE 8545
HEALTHCHECK --interval=30s --timeout=5s --retries=3 --start-period=30s \
    CMD ["fxevm", "healthcheck"]
ENTRYPOINT ["fxevm"]
