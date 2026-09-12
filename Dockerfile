FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/yukon-collector ./cmd/yukon-collector

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/yukon-collector /yukon-collector
EXPOSE 4319
ENTRYPOINT ["/yukon-collector"]
