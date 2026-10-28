FROM registry.access.redhat.com/hi/go:latest-fips-builder@sha256:f0e49909209fb085bc1371d1e3017bd8d348f89e83839786cc3285f7cb4e2046 AS builder

USER root

WORKDIR /app
COPY go.mod go.sum ./
COPY seaplane-dynamo/go.mod ./seaplane-dynamo/go.mod
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /kube-applier-aws .

FROM registry.access.redhat.com/hi/go:latest-fips@sha256:a340ab1c2fc50bf890d2cce78450161a0625c696a8c2d257da3d5a4d8b99bdf5
COPY --from=builder /kube-applier-aws /kube-applier-aws
USER 65532:65532
ENTRYPOINT ["/kube-applier-aws"]
