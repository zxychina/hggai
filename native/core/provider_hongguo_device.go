package core

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const hongguoDeviceRegisterURL = "https://log.snssdk.com/service/2/device_register/"

const hongguoDeviceRegisterHost = "log.snssdk.com"

const hongguoDeviceRegisterAttempts = 3

func hongguoAppQuery(deviceID, installID string) url.Values {
	return url.Values{
		"aid": {"8662"}, "app_name": {"novelread"}, "version_code": {"73532"}, "version_name": {"7.3.5.32"},
		"manifest_version_code": {"73532"}, "update_version_code": {"73532"}, "channel": {"update_64"},
		"device_platform": {"android"}, "os": {"android"}, "ssmix": {"a"}, "device_type": {"25053RT47C"},
		"device_brand": {"Redmi"}, "language": {"zh"}, "os_api": {"36"}, "os_version": {"16"},
		"resolution": {"1280*2772"}, "dpi": {"520"}, "ac": {"wifi"}, "device_id": {deviceID}, "iid": {installID},
	}
}

func hongguoDeviceRegistrationBody() ([]byte, error) {
	header := map[string]any{
		"display_name": "红果免费短剧", "aid": 8662, "app_version": "7.3.5.32", "version_code": 73532,
		"manifest_version_code": 73532, "update_version_code": 73532, "channel": "update_64", "package": "com.phoenix.read",
		"app_language": "zh", "os": "Android", "os_version": "16", "os_api": 36, "device_model": "25053RT47C",
		"device_brand": "Redmi", "device_manufacturer": "Xiaomi", "cpu_abi": "arm64-v8a", "density_dpi": 520,
		"display_density": "xxhdpi", "resolution": "2772x1280", "language": "zh", "timezone": 8, "access": "wifi",
		"openudid": randomHex(8), "clientudid": uuidLike(), "cdid": uuidLike(),
		"region": "CN", "tz_name": "Asia/Shanghai", "tz_offset": 28800, "sim_region": "cn", "device_platform": "android",
		"not_request_sender": 0, "is_system_app": 0, "sdk_flavor": "china", "rom": "OS2.0", "rom_version": "BP2A.250605.031.A3",
	}
	return json.Marshal(map[string]any{"magic_tag": "ss_app_log", "header": header, "_gen_time": time.Now().UnixMilli()})
}

func (downloader *Downloader) registerHongguoDevice(ctx context.Context) (string, string, string, error) {
	body, err := hongguoDeviceRegistrationBody()
	if err != nil {
		return "", "", "", err
	}
	var lastErr error
	dnsBlocked := false
	for attempt := 0; attempt < hongguoDeviceRegisterAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(time.Duration(attempt) * 500 * time.Millisecond):
			case <-ctx.Done():
				return "", "", "", errors.Join(lastErr, ctx.Err())
			}
		}
		deviceID, installID, cookie, retryable, err := downloader.registerHongguoDeviceAttempt(ctx, body, attempt > 0)
		if err == nil {
			return deviceID, installID, cookie, nil
		}
		lastErr = err
		var dnsErr *net.DNSError
		if errors.As(err, &dnsErr) {
			dnsBlocked = true
		}
		if ctx.Err() != nil || !retryable {
			break
		}
	}
	if lastErr == nil {
		lastErr = errors.New("红果设备注册失败")
	}
	if dnsBlocked {
		return "", "", "", fmt.Errorf("设备注册域名 %s 无法解析，可能被私人 DNS 或广告拦截屏蔽，请关闭私人 DNS 或更换 DNS 后重试：%w", hongguoDeviceRegisterHost, lastErr)
	}
	return "", "", "", fmt.Errorf("红果设备注册失败，请检查网络后重试：%w", lastErr)
}

func (downloader *Downloader) registerHongguoDeviceAttempt(ctx context.Context, body []byte, alternateDNS bool) (string, string, string, bool, error) {
	if alternateDNS && downloader.dns != nil {
		var lastErr error
		for _, address := range downloader.hongguoRegistrationAddresses(ctx) {
			deviceID, installID, cookie, retryable, err := downloader.registerHongguoDeviceAt(ctx, body, address)
			if err == nil {
				return deviceID, installID, cookie, true, nil
			}
			lastErr = err
			if !retryable {
				return "", "", "", false, err
			}
		}
		if lastErr != nil && ctx.Err() != nil {
			return "", "", "", false, errors.Join(lastErr, ctx.Err())
		}
	}
	return downloader.registerHongguoDeviceRequest(ctx, body)
}

func (downloader *Downloader) hongguoRegistrationAddresses(ctx context.Context) []string {
	entry, err := downloader.dns.lookupEntry(ctx, hongguoDeviceRegisterHost, "")
	if err != nil {
		return nil
	}
	if len(entry.addresses) > 2 {
		return entry.addresses[:2]
	}
	return entry.addresses
}

func (downloader *Downloader) registerHongguoDeviceRequest(ctx context.Context, body []byte) (string, string, string, bool, error) {
	request, err := hongguoDeviceRegisterRequest(ctx, body)
	if err != nil {
		return "", "", "", false, err
	}
	response, err := downloader.doPreparedCatalogRequest(request, 20*time.Second, func(prepared *http.Request) {
		query := prepared.URL.Query()
		query.Set("_rticket", strconv.FormatInt(time.Now().UnixMilli(), 10))
		prepared.URL.RawQuery = query.Encode()
	})
	if err != nil {
		return "", "", "", hongguoRegistrationRetryable(err), err
	}
	defer response.Body.Close()
	return downloader.parseHongguoDeviceRegisterResponse(response)
}

func (downloader *Downloader) registerHongguoDeviceAt(ctx context.Context, body []byte, address string) (string, string, string, bool, error) {
	timeout, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	request, err := hongguoDeviceRegisterRequest(timeout, body)
	if err != nil {
		return "", "", "", false, err
	}
	query := request.URL.Query()
	query.Set("_rticket", strconv.FormatInt(time.Now().UnixMilli(), 10))
	request.URL.RawQuery = query.Encode()
	client := hongguoPinnedClient(address)
	defer client.CloseIdleConnections()
	response, err := client.Do(request)
	if err != nil {
		return "", "", "", hongguoRegistrationRetryable(err), err
	}
	defer response.Body.Close()
	return downloader.parseHongguoDeviceRegisterResponse(response)
}

func hongguoDeviceRegisterRequest(ctx context.Context, body []byte) (*http.Request, error) {
	query := hongguoAppQuery("", "")
	query.Del("device_id")
	query.Del("iid")
	query.Set("req_id", uuidLike())
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, hongguoDeviceRegisterURL+"?"+query.Encode(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("User-Agent", hongguoAppUserAgent)
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/json; charset=utf-8")
	return request, nil
}

func hongguoPinnedClient(address string) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.ResponseHeaderTimeout = 20 * time.Second
	config := &tls.Config{MinVersion: tls.VersionTLS12}
	if transport.TLSClientConfig != nil {
		config = transport.TLSClientConfig.Clone()
		config.MinVersion = tls.VersionTLS12
	}
	transport.TLSClientConfig = config
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		if _, port, err := net.SplitHostPort(addr); err == nil {
			return dialer.DialContext(ctx, network, net.JoinHostPort(address, port))
		}
		return dialer.DialContext(ctx, network, address)
	}
	return &http.Client{Transport: transport, Timeout: 45 * time.Second}
}

func (downloader *Downloader) parseHongguoDeviceRegisterResponse(response *http.Response) (string, string, string, bool, error) {
	content, err := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	if err != nil {
		return "", "", "", true, err
	}
	if response.StatusCode != http.StatusOK {
		retryable := response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500
		return "", "", "", retryable, fmt.Errorf("红果设备注册 HTTP %d", response.StatusCode)
	}
	var result map[string]any
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.UseNumber()
	if err := decoder.Decode(&result); err != nil {
		return "", "", "", true, errors.New("红果设备注册返回格式异常")
	}
	deviceID, installID := mapString(result, "device_id_str", "device_id"), mapString(result, "install_id_str", "install_id")
	if !hongguoNumericID.MatchString(deviceID) || !hongguoNumericID.MatchString(installID) || deviceID == "0" || installID == "0" {
		return "", "", "", true, errors.New("红果设备注册未返回设备号")
	}
	cookieRequest := &http.Request{Header: http.Header{}}
	for _, cookie := range response.Cookies() {
		cookieRequest.AddCookie(cookie)
	}
	return deviceID, installID, cookieRequest.Header.Get("Cookie"), true, nil
}

func hongguoRegistrationRetryable(err error) bool {
	if err == nil {
		return false
	}
	var backoff *requestBackoff
	if errors.As(err, &backoff) {
		return false
	}
	return true
}
