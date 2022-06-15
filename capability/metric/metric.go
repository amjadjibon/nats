package metric

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/mkawserm/abesh/iface"
	"github.com/mkawserm/abesh/logger"
	"github.com/mkawserm/abesh/model"
	"github.com/mkawserm/abesh/registry"
	"github.com/nats-io/prometheus-nats-exporter/collector"
	"github.com/nats-io/prometheus-nats-exporter/exporter"
	"go.uber.org/zap"

	"github.com/amjadjibon/nats/constant"
)

type Metric struct {
	mCM               model.ConfigMap
	mExp              *exporter.NATSExporter
	mEventTransmitter iface.IEventTransmitter

	mURL                  string
	mLoggerOptions        collector.LoggerOptions
	mListenAddress        string
	mListenPort           int
	mScrapePath           string
	mGetConnz             bool
	mGetVarz              bool
	mGetSubz              bool
	mGetRoutez            bool
	mGetGatewayz          bool
	mGetLeafz             bool
	mGetReplicatorVarz    bool
	mGetStreamingChannelz bool
	mGetStreamingServerz  bool
	mGetJszFilter         string
	mRetryInterval        time.Duration
	mCertFile             string
	mKeyFile              string
	mCaFile               string
	mNATSServerURL        string
	mNATSServerTag        string
	mHTTPUser             string // User in metrics scrape by prometheus.
	mHTTPPassword         string
	mPrefix               string
	mUseInternalServerID  bool
}

func (m *Metric) GetConfigMap() model.ConfigMap {
	return m.mCM
}

func (m *Metric) SetConfigMap(cm model.ConfigMap) error {
	m.mCM = cm
	m.mURL = cm.String("url", "")
	m.mListenAddress = cm.String("listen_address", "0.0.0.0")
	m.mListenPort = cm.Int("listen_port", 7777)
	m.mScrapePath = cm.String("scrap_path", "/metrics")
	m.mGetConnz = cm.Bool("get_connz", false)
	m.mGetVarz = cm.Bool("get_varz", false)
	m.mGetSubz = cm.Bool("get_subz", false)
	m.mGetRoutez = cm.Bool("get_routez", false)
	m.mGetGatewayz = cm.Bool("get_gatewayz", false)
	m.mGetLeafz = cm.Bool("get_leafz", false)
	m.mGetReplicatorVarz = cm.Bool("get_replicator_varz", false)
	m.mGetStreamingChannelz = cm.Bool("get_streaming_channelz", false)
	m.mGetStreamingServerz = cm.Bool("get_streaming_serverz", false)
	m.mGetJszFilter = cm.String("get_jsz_filter", "")
	m.mRetryInterval = cm.Duration("retry_interval", time.Duration(exporter.DefaultRetryIntervalSecs))
	m.mCertFile = cm.String("cert_file", "")
	m.mKeyFile = cm.String("key_file", "")
	m.mCaFile = cm.String("ca_file", "")
	m.mNATSServerURL = cm.String("nats_server_url", "")
	m.mNATSServerTag = cm.String("nats_server_tag", "")
	m.mHTTPUser = cm.String("http_user", "")
	m.mHTTPPassword = cm.String("http_password", "")
	m.mPrefix = cm.String("prefix", "")
	m.mUseInternalServerID = cm.Bool("user_internal_server_id", false)
	return nil
}

func (m *Metric) Setup() error {
	var opts = exporter.GetDefaultExporterOptions()
	opts.ListenAddress = m.mListenAddress
	opts.ListenPort = m.mListenPort
	opts.ScrapePath = m.mScrapePath
	opts.GetConnz = m.mGetConnz
	opts.GetVarz = m.mGetVarz
	opts.GetSubz = m.mGetSubz
	opts.GetRoutez = m.mGetRoutez
	opts.GetGatewayz = m.mGetGatewayz
	opts.GetLeafz = m.mGetLeafz
	opts.GetReplicatorVarz = m.mGetReplicatorVarz
	opts.GetStreamingChannelz = m.mGetStreamingChannelz
	opts.GetStreamingServerz = m.mGetStreamingServerz
	opts.GetJszFilter = m.mGetJszFilter
	opts.RetryInterval = m.mRetryInterval
	opts.CertFile = m.mCertFile
	opts.KeyFile = m.mKeyFile
	opts.CaFile = m.mCaFile
	opts.NATSServerURL = m.mNATSServerURL
	opts.NATSServerTag = m.mNATSServerTag
	opts.HTTPUser = m.mHTTPUser
	opts.HTTPPassword = m.mHTTPPassword
	opts.Prefix = m.mPrefix
	opts.UseInternalServerID = m.mUseInternalServerID

	var id = collector.GetServerIDFromVarz(m.mURL, opts.RetryInterval)
	var exp = exporter.NewExporter(opts)
	err := exp.AddServer(id, m.mURL)
	if err != nil {
		return err
	}

	m.mExp = exp
	return nil
}

func (m *Metric) Name() string {
	return Name
}

func (m *Metric) Version() string {
	return constant.NatsVersion
}

func (m *Metric) Category() string {
	return Category
}

func (m *Metric) ContractId() string {
	return ContractId
}

func (m *Metric) New() iface.ICapability {
	return &Metric{}
}

func (m *Metric) Start(ctx context.Context) error {
	logger.L(m.ContractId()).Debug("exporter starting...")
	return m.mExp.Start()
}

func (m *Metric) Stop(ctx context.Context) error {
	m.mExp.Stop()
	return nil
}

func (m *Metric) SetEventTransmitter(eventTransmitter iface.IEventTransmitter) error {
	m.mEventTransmitter = eventTransmitter
	return nil
}

func (m *Metric) GetEventTransmitter() iface.IEventTransmitter {
	return m.mEventTransmitter
}

func (m *Metric) TransmitInputEvent(contractId string, event *model.Event) error {
	if m.GetEventTransmitter() != nil {
		go func() {
			var err = m.GetEventTransmitter().TransmitInputEvent(contractId, event)
			if err != nil {
				logger.L(m.ContractId()).Error(err.Error(),
					zap.String("version", m.Version()),
					zap.String("name", m.Name()),
					zap.String("contract_id", m.ContractId()),
				)
			}
		}()
	}
	return nil
}

func (m *Metric) TransmitOutputEvent(contractId string, event *model.Event) error {
	if m.GetEventTransmitter() != nil {
		go func() {
			err := m.GetEventTransmitter().TransmitOutputEvent(contractId, event)
			if err != nil {
				logger.L(m.ContractId()).Error(err.Error(),
					zap.String("version", m.Version()),
					zap.String("name", m.Name()),
					zap.String("contract_id", m.ContractId()),
				)
			}
		}()
	}
	return nil
}

func (m *Metric) AddAuthorizer(authorizer iface.IAuthorizer, authorizerExpression string, method string) error {
	return nil
}

func init() {
	registry.GlobalRegistry().AddCapability(&Metric{})
}

// parseServerIDAndURL parses the url argument the optional id for the server ID.
func parseServerIDAndURL(urlArg string) (string, string, error) {
	var id string
	var monURL string

	// if there is an optional tag, parse it out and check the url
	if strings.Contains(urlArg, ",") {
		idx := strings.LastIndex(urlArg, ",")
		id = urlArg[:idx]
		monURL = urlArg[idx+1:]
		if _, err := url.ParseRequestURI(monURL); err != nil {
			return "", "", err
		}
	} else {
		// The URL is the basis for a default id with credentials stripped out.
		u, err := url.ParseRequestURI(urlArg)
		if err != nil {
			return "", "", err
		}
		id = fmt.Sprintf("%s://%s", u.Scheme, u.Host)
		monURL = urlArg
	}
	return id, monURL, nil
}
