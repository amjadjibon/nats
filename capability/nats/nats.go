package nats

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"time"

	"github.com/mkawserm/abesh/iface"
	"github.com/mkawserm/abesh/logger"
	"github.com/mkawserm/abesh/model"
	"github.com/mkawserm/abesh/registry"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"go.uber.org/zap"

	"github.com/amjadjibon/nats/constant"
)

type Nats struct {
	mCM               model.ConfigMap
	mServer           *server.Server
	mEventTransmitter iface.IEventTransmitter

	/* Nats Server Options */
	mConfigFile                 string
	mServerName                 string
	mHost                       string
	mPort                       int
	mClientAdvertise            string
	mTrace                      bool
	mDebug                      bool
	mTraceVerbose               bool
	mNoLog                      bool
	mNoSigs                     bool
	mNoSublistCache             bool
	mNoHeaderSupport            bool
	mDisableShortFirstPing      bool
	mLogtime                    bool
	mMaxConn                    int
	mMaxSubs                    int
	mMaxSubTokens               uint8
	mNKeys                      []*server.NkeyUser
	mUsers                      []*server.User
	mAccounts                   []*server.Account
	mNoAuthUser                 string
	mSystemAccount              string
	mNoSystemAccount            bool
	mUsername                   string
	mPassword                   string
	mAuthorization              string
	mPingInterval               time.Duration
	mMaxPingsOut                int
	mHttpHost                   string
	mHttpPort                   int
	mHttpBasePath               string
	mHttpsPort                  int
	mAuthTimeout                float64
	mMaxControlLine             int32
	mMaxPayload                 int32
	mMaxPending                 int64
	mCluster                    server.ClusterOpts
	mGateway                    server.GatewayOpts
	mLeafNode                   server.LeafNodeOpts
	mJetStream                  bool
	mJetStreamMaxMemory         int64
	mJetStreamMaxStore          int64
	mJetStreamDomain            string
	mJetStreamExtHint           string
	mJetStreamKey               string
	mJetStreamUniqueTag         string
	mJetStreamLimits            server.JSLimitOpts
	mStoreDir                   string
	mJsAccDefaultDomain         map[string]string
	mWebsocket                  server.WebsocketOpts
	mMQTT                       server.MQTTOpts
	mProfPort                   int
	mPidFile                    string
	mPortsFileDir               string
	mLogFile                    string
	mLogSizeLimit               int64
	mSyslog                     bool
	mRemoteSyslog               string
	mRoutes                     []*url.URL
	mRoutesStr                  string
	mTLSTimeout                 float64
	mTLS                        bool
	mTLSVerify                  bool
	mTLSMap                     bool
	mTLSCert                    string
	mTLSKey                     string
	mTLSCaCert                  string
	mTLSConfig                  *tls.Config
	mTLSPinnedCerts             server.PinnedCertSet
	mTLSRateLimit               int64
	mAllowNonTLS                bool
	mWriteDeadline              time.Duration
	mMaxClosedClients           int
	mLameDuckDuration           time.Duration
	mLameDuckGracePeriod        time.Duration
	mMaxTracedMsgLen            int
	mTrustedKeys                []string
	mTrustedOperators           []*jwt.OperatorClaims
	mAccountResolver            server.AccountResolver
	mAccountResolverTLSConfig   *tls.Config
	mAlwaysEnableNonce          bool
	mCustomClientAuthentication server.Authentication
	mCustomRouterAuthentication server.Authentication
	mCheckConfig                bool
	mConnectErrorReports        int
	mReconnectErrorReports      int
	mTags                       jwt.TagList
	mOCSPConfig                 *server.OCSPConfig
	/* Nats Server Options */
}

func (n *Nats) Name() string {
	return Name
}

func (n *Nats) Version() string {
	return constant.NatsVersion
}

func (n *Nats) Category() string {
	return Category
}

func (n *Nats) ContractId() string {
	return ContractId
}

func (n *Nats) New() iface.ICapability {
	return &Nats{}
}

func (n *Nats) GetConfigMap() model.ConfigMap {
	return n.mCM
}

func (n *Nats) SetConfigMap(cm model.ConfigMap) error {
	n.mCM = cm
	n.mConfigFile = cm.String("config_file", "")
	n.mServerName = cm.String("server_name", "")
	n.mHost = cm.String("host", "")
	n.mPort = cm.Int("port", 0)
	n.mClientAdvertise = cm.String("client_advertise", "")
	n.mTrace = cm.Bool("trace", false)
	n.mDebug = cm.Bool("debug", false)
	n.mTraceVerbose = cm.Bool("trace_verbose", false)
	n.mNoLog = cm.Bool("no_log", false)
	n.mNoSigs = cm.Bool("no_sigs", false)
	n.mNoSublistCache = cm.Bool("no_sublist_cache", false)
	n.mNoHeaderSupport = cm.Bool("no_header_support", false)
	n.mDisableShortFirstPing = cm.Bool("disable_short_first_ping", false)
	n.mLogtime = cm.Bool("logtime", false)
	n.mMaxConn = cm.Int("max_connections", 0)
	n.mMaxSubs = cm.Int("max_subscriptions", 0)
	n.mMaxSubTokens = cm.Uint8("max_sub_tokens", 0)
	n.mNKeys = []*server.NkeyUser{}
	n.mUsers = []*server.User{}
	n.mAccounts = []*server.Account{}
	n.mNoAuthUser = cm.String("no_auth_user", "")
	n.mSystemAccount = cm.String("system_account", "")
	n.mNoSystemAccount = cm.Bool("no_system_account", false)
	n.mUsername = cm.String("username", "")
	n.mPassword = cm.String("password", "")
	n.mAuthorization = cm.String("authorization", "")
	n.mPingInterval = cm.Duration("ping_interval", 0)
	n.mMaxPingsOut = cm.Int("max_pings_out", 0)
	n.mHttpHost = cm.String("http_host", "")
	n.mHttpPort = cm.Int("http_port", 0)
	n.mHttpBasePath = cm.String("http_base_path", "")
	n.mHttpsPort = cm.Int("https_port", 0)
	n.mAuthTimeout = cm.Float64("auth_timeout", 0)
	n.mMaxControlLine = cm.Int32("max_control_line", 0)
	n.mMaxPayload = cm.Int32("max_payload", 0)
	n.mMaxPending = cm.Int64("max_pending", 0)
	n.mCluster = server.ClusterOpts{}
	n.mGateway = server.GatewayOpts{}
	n.mLeafNode = server.LeafNodeOpts{}
	n.mJetStream = cm.Bool("jetstream", false)
	n.mJetStreamMaxMemory = cm.Int64("jetstream_max_memory", 0)
	n.mJetStreamMaxStore = cm.Int64("jetstream_max_store", 0)
	n.mJetStreamDomain = cm.String("jetstream_domain", "")
	n.mJetStreamExtHint = cm.String("jetstream_ext_hint", "")
	n.mJetStreamKey = cm.String("jetstream_key", "")
	n.mJetStreamUniqueTag = cm.String("jetstream_unique_tag", "")
	n.mJetStreamLimits = server.JSLimitOpts{}
	n.mStoreDir = cm.String("store_dir", "")
	n.mJsAccDefaultDomain = cm.StringMap("js_acc_default_domain", nil)
	n.mWebsocket = server.WebsocketOpts{}
	n.mMQTT = server.MQTTOpts{}
	n.mProfPort = cm.Int("prof_port", 0)
	n.mPidFile = cm.String("pid_file", "")
	n.mPortsFileDir = cm.String("ports_file_dir", "")
	n.mLogFile = cm.String("log_file", "")
	n.mLogSizeLimit = cm.Int64("log_size_limit", 0)
	n.mSyslog = cm.Bool("syslog", false)
	n.mRemoteSyslog = cm.String("remote_syslog", "")
	n.mRoutes = []*url.URL{}
	n.mRoutesStr = cm.String("routes_str", "")
	n.mTLSTimeout = cm.Float64("tls_timeout", 0)
	n.mTLSMap = cm.Bool("tls", false)
	n.mTLSVerify = cm.Bool("tls_verify", false)
	n.mTLSMap = cm.Bool("tls", false)
	n.mTLSCert = cm.String("tls_cert", "")
	n.mTLSKey = cm.String("tls_key", "")
	n.mTLSCaCert = cm.String("tls_ca_cert", "")
	n.mTLSConfig = &tls.Config{}
	n.mTLSPinnedCerts = server.PinnedCertSet{}
	n.mTLSRateLimit = cm.Int64("tls_rate_limit", 0)
	n.mAllowNonTLS = cm.Bool("allow_non_tls", false)
	n.mWriteDeadline = cm.Duration("write_deadline", 0)
	n.mMaxClosedClients = cm.Int("max_closed_clients", 0)
	n.mLameDuckDuration = cm.Duration("lame_duck_duration", 0)
	n.mLameDuckGracePeriod = cm.Duration("lame_duck_grace_period", 0)
	n.mMaxTracedMsgLen = cm.Int("max_traced_msg_len", 0)
	n.mTrustedKeys = cm.StringList("trusted_keys", ",", []string{})
	n.mTrustedOperators = []*jwt.OperatorClaims{}
	n.mAccountResolver = nil
	n.mAccountResolverTLSConfig = &tls.Config{}
	n.mAlwaysEnableNonce = cm.Bool("always_enable_nonce", false)
	n.mCustomClientAuthentication = nil
	n.mCustomRouterAuthentication = nil
	n.mCheckConfig = cm.Bool("check_config", false)
	n.mConnectErrorReports = cm.Int("connect_error_reports", 0)
	n.mReconnectErrorReports = cm.Int("reconnect_error_reports", 0)
	n.mTags = jwt.TagList{}
	n.mOCSPConfig = &server.OCSPConfig{}
	return nil
}

func (n *Nats) Setup() error {
	var opts = &server.Options{
		ConfigFile:                 n.mConfigFile,
		ServerName:                 n.mServerName,
		Host:                       n.mHost,
		Port:                       n.mPort,
		ClientAdvertise:            n.mClientAdvertise,
		Trace:                      n.mTrace,
		Debug:                      n.mDebug,
		TraceVerbose:               n.mTraceVerbose,
		NoLog:                      n.mNoLog,
		NoSigs:                     n.mNoSigs,
		NoSublistCache:             n.mNoSublistCache,
		NoHeaderSupport:            n.mNoHeaderSupport,
		DisableShortFirstPing:      n.mDisableShortFirstPing,
		Logtime:                    n.mLogtime,
		MaxConn:                    n.mMaxConn,
		MaxSubs:                    n.mMaxSubs,
		MaxSubTokens:               n.mMaxSubTokens,
		Nkeys:                      n.mNKeys,
		Users:                      n.mUsers,
		Accounts:                   n.mAccounts,
		NoAuthUser:                 n.mNoAuthUser,
		SystemAccount:              n.mSystemAccount,
		NoSystemAccount:            n.mNoSystemAccount,
		Username:                   n.mUsername,
		Password:                   n.mPassword,
		Authorization:              n.mAuthorization,
		PingInterval:               n.mPingInterval,
		MaxPingsOut:                n.mMaxPingsOut,
		HTTPHost:                   n.mHttpHost,
		HTTPPort:                   n.mHttpPort,
		HTTPBasePath:               n.mHttpBasePath,
		HTTPSPort:                  n.mHttpsPort,
		AuthTimeout:                n.mAuthTimeout,
		MaxControlLine:             n.mMaxControlLine,
		MaxPayload:                 n.mMaxPayload,
		MaxPending:                 n.mMaxPending,
		Cluster:                    n.mCluster,
		Gateway:                    n.mGateway,
		LeafNode:                   n.mLeafNode,
		JetStream:                  n.mJetStream,
		JetStreamMaxMemory:         n.mJetStreamMaxMemory,
		JetStreamMaxStore:          n.mJetStreamMaxStore,
		JetStreamDomain:            n.mJetStreamDomain,
		JetStreamExtHint:           n.mJetStreamExtHint,
		JetStreamKey:               n.mJetStreamKey,
		JetStreamUniqueTag:         n.mJetStreamUniqueTag,
		JetStreamLimits:            n.mJetStreamLimits,
		StoreDir:                   n.mStoreDir,
		JsAccDefaultDomain:         n.mJsAccDefaultDomain,
		Websocket:                  n.mWebsocket,
		MQTT:                       n.mMQTT,
		ProfPort:                   n.mProfPort,
		PidFile:                    n.mPidFile,
		PortsFileDir:               n.mPortsFileDir,
		LogFile:                    n.mLogFile,
		LogSizeLimit:               n.mLogSizeLimit,
		Syslog:                     n.mSyslog,
		RemoteSyslog:               n.mRemoteSyslog,
		Routes:                     n.mRoutes,
		RoutesStr:                  n.mRoutesStr,
		TLSTimeout:                 n.mTLSTimeout,
		TLS:                        n.mTLS,
		TLSVerify:                  n.mTLSVerify,
		TLSMap:                     n.mTLSMap,
		TLSCert:                    n.mTLSCert,
		TLSKey:                     n.mTLSKey,
		TLSCaCert:                  n.mTLSCaCert,
		TLSConfig:                  n.mTLSConfig,
		TLSPinnedCerts:             n.mTLSPinnedCerts,
		TLSRateLimit:               n.mTLSRateLimit,
		AllowNonTLS:                n.mAllowNonTLS,
		WriteDeadline:              n.mWriteDeadline,
		MaxClosedClients:           n.mMaxClosedClients,
		LameDuckDuration:           n.mLameDuckDuration,
		LameDuckGracePeriod:        n.mLameDuckGracePeriod,
		MaxTracedMsgLen:            n.mMaxTracedMsgLen,
		TrustedKeys:                n.mTrustedKeys,
		TrustedOperators:           n.mTrustedOperators,
		AccountResolver:            n.mAccountResolver,
		AccountResolverTLSConfig:   n.mAccountResolverTLSConfig,
		AlwaysEnableNonce:          n.mAlwaysEnableNonce,
		CustomClientAuthentication: n.mCustomClientAuthentication,
		CustomRouterAuthentication: n.mCustomRouterAuthentication,
		CheckConfig:                n.mCheckConfig,
		ConnectErrorReports:        n.mConnectErrorReports,
		ReconnectErrorReports:      n.mReconnectErrorReports,
		Tags:                       n.mTags,
		OCSPConfig:                 n.mOCSPConfig,
	}

	// Create the server with appropriate options.
	srv, err := server.NewServer(opts)
	if err != nil {
		server.PrintAndDie(fmt.Sprintf("%s", err))
	}

	srv.ConfigureLogger()

	n.mServer = srv
	return nil
}

func (n *Nats) Start(ctx context.Context) error {
	n.mServer.Start()
	return nil
}

func (n *Nats) Stop(ctx context.Context) error {
	n.mServer.WaitForShutdown()
	n.mServer.Shutdown()
	return nil
}

func (n *Nats) SetEventTransmitter(eventTransmitter iface.IEventTransmitter) error {
	n.mEventTransmitter = eventTransmitter
	return nil
}

func (n *Nats) GetEventTransmitter() iface.IEventTransmitter {
	return n.mEventTransmitter
}

func (n *Nats) TransmitInputEvent(contractId string, event *model.Event) error {
	if n.GetEventTransmitter() != nil {
		go func() {
			var err = n.GetEventTransmitter().TransmitInputEvent(contractId, event)
			if err != nil {
				logger.L(n.ContractId()).Error(err.Error(),
					zap.String("version", n.Version()),
					zap.String("name", n.Name()),
					zap.String("contract_id", n.ContractId()))
			}

		}()
	}
	return nil
}

func (n *Nats) TransmitOutputEvent(contractId string, event *model.Event) error {
	if n.GetEventTransmitter() != nil {
		go func() {
			err := n.GetEventTransmitter().TransmitOutputEvent(contractId, event)
			if err != nil {
				logger.L(n.ContractId()).Error(err.Error(),
					zap.String("version", n.Version()),
					zap.String("name", n.Name()),
					zap.String("contract_id", n.ContractId()))
			}
		}()
	}
	return nil
}

func (n *Nats) AddAuthorizer(authorizer iface.IAuthorizer, authorizerExpression string, method string) error {
	return nil
}

func init() {
	registry.GlobalRegistry().AddCapability(&Nats{})
}
