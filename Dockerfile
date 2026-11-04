FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG SERVICE=api
RUN CGO_ENABLED=0 go build -trimpath -o /vaultstore ./cmd/${SERVICE}
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /vaultstore /vaultstore
EXPOSE 8080
ENTRYPOINT ["/vaultstore"]
