FROM golang:1.27-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/wecom-dify ./cmd/wecom-dify && mkdir -p /state

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/wecom-dify /wecom-dify
COPY --chown=65532:65532 --from=build /state /data
WORKDIR /data
ENV SQLITE_PATH=/data/state.db
EXPOSE 8080
ENTRYPOINT ["/wecom-dify"]
