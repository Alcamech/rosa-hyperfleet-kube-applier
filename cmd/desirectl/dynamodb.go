package main

import (
	"context"
	"fmt"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/openshift-online/rosa-hyperfleet-kube-applier/internal/database"
)

type clientPair struct {
	specsDB  database.KubeApplierDBClient
	statusDB database.KubeApplierDBClient
}

func newClientPair(ctx context.Context, activeCtx *Context) (*clientPair, error) {
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(activeCtx.AWSRegion),
	}
	if activeCtx.EndpointURL != "" {
		opts = append(opts, awsconfig.WithBaseEndpoint(activeCtx.EndpointURL))
		// Use dummy credentials for LocalStack / dev environments.
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}

	client := dynamodb.NewFromConfig(cfg)
	specsPrefix := activeCtx.EffectiveSpecsPrefix()
	statusPrefix := activeCtx.EffectiveStatusPrefix()

	return &clientPair{
		specsDB:  database.NewDynamoDBKubeApplierDBClient(client, client, specsPrefix, specsPrefix),
		statusDB: database.NewDynamoDBKubeApplierDBClient(client, client, statusPrefix, statusPrefix),
	}, nil
}

func (cp *clientPair) Close() {
	cp.specsDB.Close()
	cp.statusDB.Close()
}
