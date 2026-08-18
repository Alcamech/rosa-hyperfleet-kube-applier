FROM registry.access.redhat.com/ubi9/ubi-minimal:9.8-1786987521 AS builder

USER root
RUN microdnf install -y golang git && microdnf clean all

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /kube-applier-aws .

FROM registry.access.redhat.com/ubi9/ubi-minimal:9.8-1786987521
COPY --from=builder /kube-applier-aws /kube-applier-aws
USER 65532:65532
ENTRYPOINT ["/kube-applier-aws"]
