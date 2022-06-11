package kv

import (
	"context"
	"time"

	encodingIface "github.com/amjadjibon/encoding/iface"
	encodingRegistry "github.com/amjadjibon/encoding/registry"
	"github.com/mkawserm/abesh/iface"
	"github.com/mkawserm/abesh/model"
	"github.com/mkawserm/abesh/registry"
	"github.com/nats-io/nats.go"

	"github.com/amjadjibon/nats/constant"
)

type KV struct {
	mCM                   model.ConfigMap
	mKV                   nats.KeyValue
	mKVBucket             string
	mKVBucketDescription  string
	mKVBucketMaxValueSize int32
	mKVBucketHistory      uint8
	mKVBucketTTL          time.Duration
	mKVBucketMaxBytes     int64
	mKVBucketStorage      int
	mKVBucketReplicas     int
	mKVBucketPlacement    []string
	mEncoding             encodingIface.IEncoding
	mEncodingName         string
	mNatsUrl              string
	mClientName           string
	mUsername             string
	mPassword             string
	mMaxReconnects        int
	mReconnectWait        time.Duration
	mTimeout              time.Duration
	mReconnectJitter      time.Duration
	mReconnectJitterTLS   time.Duration
	mPingInterval         time.Duration
	mMaxPingOut           int
	mReconnectBufSize     int
	mDrainTimeout         time.Duration
	mCapabilityRegistry   iface.ICapabilityRegistry
}

func (k *KV) Name() string {
	return Name
}

func (k *KV) Version() string {
	return constant.NatsVersion
}

func (k *KV) Category() string {
	return Category
}

func (k *KV) ContractId() string {
	return ContractId
}

func (k *KV) New() iface.ICapability {
	return &KV{}
}

func (k *KV) SetConfigMap(cm model.ConfigMap) error {
	k.mCM = cm
	k.mKVBucket = cm.String("kv_bucket", "kvstore")
	k.mKVBucketDescription = cm.String("kv_bucket_description", "nats kvstore")
	k.mKVBucketMaxValueSize = cm.Int32("kv_bucket_max_value_size", 0)
	k.mKVBucketHistory = cm.Uint8("kv_bucket_history", 0)
	k.mKVBucketTTL = cm.Duration("kv_bucket_ttl", 0)
	k.mKVBucketMaxBytes = cm.Int64("kv_bucket_max_bytes", 0)
	k.mKVBucketStorage = cm.Int("kv_bucket_storage", 0)
	k.mKVBucketReplicas = cm.Int("kv_bucket_replicas", 0)
	k.mClientName = cm.String("client_name", "abesh_nats_kv")
	k.mUsername = cm.String("username", "")
	k.mPassword = cm.String("password", "")
	k.mNatsUrl = cm.String("nats_url", "nats://localhost:4222")
	k.mEncodingName = cm.String("encoding", "json")
	k.mMaxReconnects = cm.Int("max_reconnects", nats.DefaultMaxReconnect)
	k.mReconnectWait = cm.Duration("reconnect_wait", nats.DefaultReconnectWait)
	k.mTimeout = cm.Duration("timeout", nats.DefaultTimeout)
	k.mReconnectJitter = cm.Duration("reconnect_jitter", nats.DefaultReconnectJitter)
	k.mReconnectJitterTLS = cm.Duration("reconnect_jitter_tls", nats.DefaultReconnectJitterTLS)
	k.mPingInterval = cm.Duration("ping_interval", nats.DefaultPingInterval)
	k.mMaxPingOut = cm.Int("max_ping_out", nats.DefaultMaxPingOut)
	k.mReconnectBufSize = cm.Int("reconnect_buf_size", nats.DefaultReconnectBufSize)
	k.mDrainTimeout = cm.Duration("drain_timeout", nats.DefaultDrainTimeout)
	return nil
}

func (k *KV) GetConfigMap() model.ConfigMap {
	return k.mCM
}

func (k *KV) getKV() (nats.KeyValue, error) {
	var opts []nats.Option
	opts = append(opts, nats.Name(k.mClientName))
	opts = append(opts, nats.MaxReconnects(k.mMaxReconnects))
	opts = append(opts, nats.ReconnectWait(k.mReconnectWait))
	opts = append(opts, nats.Timeout(k.mTimeout))
	opts = append(opts, nats.ReconnectJitter(k.mReconnectJitter, k.mReconnectJitterTLS))
	opts = append(opts, nats.PingInterval(k.mPingInterval))
	opts = append(opts, nats.MaxPingsOutstanding(k.mMaxPingOut))
	opts = append(opts, nats.ReconnectBufSize(k.mReconnectBufSize))
	opts = append(opts, nats.DrainTimeout(k.mDrainTimeout))
	opts = append(opts, nats.UserInfo(k.mUsername, k.mPassword))
	connect, err := nats.Connect(k.mNatsUrl, opts...)
	for err != nil {
		return nil, err
	}
	stream, err := connect.JetStream()
	if err != nil {
		return nil, err
	}
	kv, err := stream.KeyValue(k.mKVBucket)
	if err == nil {
		return kv, nil
	}
	if err == nats.ErrBucketNotFound {
		kv, err = stream.CreateKeyValue(&nats.KeyValueConfig{
			Bucket:       k.mKVBucket,
			Description:  k.mKVBucketDescription,
			MaxValueSize: k.mKVBucketMaxValueSize,
			History:      k.mKVBucketHistory,
			TTL:          k.mKVBucketTTL,
			MaxBytes:     k.mKVBucketMaxBytes,
			Storage:      nats.StorageType(k.mKVBucketStorage),
			Replicas:     k.mKVBucketReplicas,
		})
		if err == nil {
			return kv, nil
		}
	}
	return nil, err
}

func (k *KV) SetKV() error {
	if k.mKV != nil {
		return nil
	}

	kv, err := k.getKV()
	if err != nil {
		return err
	}

	k.mKV = kv
	
	return nil
}

func (k *KV) Setup() error {
	k.mEncoding = encodingRegistry.EncodingRegistry().GetEncoding(k.mEncodingName)
	if k.mEncoding == nil {
		panic("encoding not found")
	}
	return nil
}

func (k *KV) Get(ctx context.Context, key string, value interface{}) error {
	if err := k.SetKV(); err != nil {
		return err
	}

	get, err := k.mKV.Get(key)
	if err == nil {
		return err
	}

	return k.mEncoding.Unmarshal(get.Value(), value)
}

func (k *KV) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	if err := k.SetKV(); err != nil {
		return err
	}

	var data, err = k.mEncoding.Marshal(value)
	if err != nil {
		return err
	}

	_, err = k.mKV.Create(key, data)
	if err != nil {
		return err
	}

	return nil
}

func (k *KV) Delete(ctx context.Context, key string) error {
	if err := k.SetKV(); err != nil {
		return err
	}

	return k.mKV.Delete(key)
}

func init() {
	registry.GlobalRegistry().AddCapability(&KV{})
}
