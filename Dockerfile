FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/yukon-collector ./cmd/yukon-collector

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/yukon-collector /yukon-collector
USER nonroot:nonroot
EXPOSE 4319
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 CMD ["/yukon-collector", "healthcheck"]
ENTRYPOINT ["/yukon-collector"]
