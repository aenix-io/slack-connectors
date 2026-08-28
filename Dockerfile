FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
# Which connector this image is. Both binaries share the core, so they share a
# Dockerfile; the tag says which one came out.
ARG CMD=bridge
RUN CGO_ENABLED=0 go build \
      -ldflags "-X github.com/aenix-io/slack-connectors/internal/version.Version=${VERSION}" \
      -o /out/bridge ./cmd/${CMD}

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/bridge /bridge
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/bridge"]
